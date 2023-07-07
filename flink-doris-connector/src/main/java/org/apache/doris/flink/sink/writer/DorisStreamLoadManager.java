package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.HttpUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

public class DorisStreamLoadManager {
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriter.class);

    private ConcurrentHashMap<Long, RecordBufferCache> cpkDataCache;
    private DorisStreamLoadImpl dorisStreamLoadImpl;
    private boolean loadBatchFirstRecord;
    private volatile boolean loading;
    private StreamLoadPara para;
    private String labelPrefix;
    private LabelGenerator labelGenerator;
    private transient ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopReceiveData = false;
    // current committed checkpoint
    private long curCheckpointId = -1;
    // current checkpoint id which is writing
    private long curLoadingCheckpoint = -1;
    // used to interrupt the thread when the error is Fatal
    private transient Thread executorThread;
    private boolean init = false;

    private static class SingletonHolder {
        private static final DorisStreamLoadManager INSTANCE = new DorisStreamLoadManager();
    }

    public static DorisStreamLoadManager getDorisStreamLoadManager() {
        return SingletonHolder.INSTANCE;
    }

    private DorisStreamLoadManager(){

    }

    public void init(long taskId, StreamLoadPara para){
        cpkDataCache = new ConcurrentHashMap<Long, RecordBufferCache>();
        loadBatchFirstRecord = true;
        this.para = para;
        this.labelPrefix = para.labelPrefix + "_" + taskId;
        this.labelGenerator = new LabelGenerator(labelPrefix, para.executionOptions.enabled2PC());
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("stream-load-check"));
        this.curCheckpointId = para.lastCheckpointId;
        this.init = true;
    }

    public boolean isInit() {
        return this.init;
    }

    /**
     * 1, init the stream load,
     * 2, prepare the load,
     * 3,start the new stream load
     * @param state
     * @throws IOException
     */
    public void initializeLoad(List<DorisWriterState> state) throws IOException {
        try {
            this.dorisStreamLoadImpl = new DorisStreamLoadImpl(
                    RestService.getBackend(para.dorisOptions, para.dorisReadOptions, LOG),
                    para.dorisOptions,
                    para.executionOptions,
                    labelGenerator, new HttpUtil().getHttpClient());
            // TODO: we need check and abort all pending transaction.
            //  Discard transactions that may cause the job to fail.
            if(para.executionOptions.enabled2PC()) {
                dorisStreamLoadImpl.abortPreCommit(labelPrefix, curCheckpointId + 1);
            }
        } catch (Exception e) {
            throw new DorisRuntimeException(e);
        }
        // get main work thread.
        executorThread = Thread.currentThread();
        dorisStreamLoadImpl.startLoad(labelGenerator.generateLabel(curCheckpointId + 1),
                                          getCurrentStreamDataCache(curCheckpointId + 1));
        curLoadingCheckpoint = curCheckpointId + 1;
        // when uploading data in streaming mode, we need to regularly detect whether there are exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(this::checkDone, 200,
                                                            para.executionOptions.checkInterval(),
                                                            TimeUnit.MILLISECONDS);
    }

    /**
     * stop the stream load
     * @return
     * @throws IOException
     */
    public RespContent stopLoad() throws IOException{
        return dorisStreamLoadImpl.stopLoad();
    }

    /**
     * start a new stream load
     * @param label
     * @param checkpointId
     * @throws IOException
     */
    public void startLoad(String label, long checkpointId) throws IOException{
        dorisStreamLoadImpl.startLoad(label, getCurrentStreamDataCache(checkpointId));
    }

    public void close() throws IOException {
        dorisStreamLoadImpl.close();
    }

    private void checkDone() {
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        LOG.debug("start timer checker, interval {} ms", para.executionOptions.checkInterval());
        if (dorisStreamLoadImpl.getPendingLoadFuture() != null
                && dorisStreamLoadImpl.getPendingLoadFuture().isDone()) {
            if (!loading) {
                LOG.debug("not loading, skip timer checker");
                return;
            }

            // double check the future, to avoid getting the old future
            if (dorisStreamLoadImpl.getPendingLoadFuture() != null
                    && dorisStreamLoadImpl.getPendingLoadFuture().isDone()) {
                // error happened when loading, now we should stop receive data
                // and abort previous txn(stream load) and start a new txn(stream load)
                // use send cached data to new txn, then notify to restart the stream
                try {
                    // abort previous txn(stream load)
                    abortUnCommittedTxn();

                    // use anther be to load

                    this.dorisStreamLoadImpl = new DorisStreamLoadImpl(
                            RestService.getBackend(para.dorisOptions, para.dorisReadOptions, LOG),
                            para.dorisOptions,
                            para.executionOptions,
                            labelGenerator, new HttpUtil().getHttpClient());
                    // TODO: we need check and abort all pending transaction.
                    //  Discard transactions that may cause the job to fail.
                    if(para.executionOptions.enabled2PC()) {
                        dorisStreamLoadImpl.abortPreCommit(labelPrefix, curCheckpointId + 1);
                    }
                } catch (Exception e) {
                    throw new DorisRuntimeException(e);
                }

                try {
                    // start a new txn(stream load)
                    dorisStreamLoadImpl.startLoad(labelGenerator.generateLabel(curCheckpointId + 1),
                                                      getCurrentStreamDataCache(curCheckpointId + 1));
                    curLoadingCheckpoint = curCheckpointId + 1;
                } catch (Exception e) {
                    throw new DorisRuntimeException(e);
                }
                // now restart the data stream
                stopReceiveData = false;
            }
        }
    }


    public void writeRecord(byte[] buf, long cpk) throws IOException {
        dorisStreamLoadImpl.writeRecord(buf);
    }

    public RecordBufferCache getCurrentStreamDataCache(long cpk) {
        if (!cpkDataCache.contains(cpk)) {
            cpkDataCache.put(cpk, new RecordBufferCache(para.executionOptions.getBufferSize()));
        }
        return cpkDataCache.get(cpk);
    }

    public RecordBufferCache getStreamDataCache(long cpk) {
        if (!cpkDataCache.contains(cpk)) {
            cpkDataCache.put(cpk, new RecordBufferCache(para.executionOptions.getBufferSize()));
        }
        return cpkDataCache.get(cpk);
    }

    public void remove(long cpk) {
        // TODO recycle the RecordBufferCache
        if(cpkDataCache.contains(cpk)) {
            //recycle all buffer to ByteBufferManager
            cpkDataCache.get(cpk).recycle();
            cpkDataCache.remove(cpk);
            // update current committed checkpoint id
            this.curCheckpointId = cpk;
        }
    }

    public void abortUnCommittedTxn() throws Exception {
        long leastCheckpointId = -1;
        for(Long cpk: cpkDataCache.keySet()) {
            if (leastCheckpointId == -1) {
                leastCheckpointId = cpk.longValue();
            }

            if(leastCheckpointId>cpk.longValue()) {
                leastCheckpointId = cpk.longValue();
            }
        }

        dorisStreamLoadImpl.abortPreCommit("", leastCheckpointId);
    }

    public void setLoading(boolean loading) {
        this.loading = loading;
    }

    public boolean enabled2PC() {
        return this.para.executionOptions.enabled2PC();
    }

    public boolean isStopReceiveData() {
        return this.stopReceiveData;
    }

    public String getDb() {
        return this.dorisStreamLoadImpl.getDb();
    }

    public String getHostPort() {
        return this.dorisStreamLoadImpl.getHostPort();
    }

    public void setDorisStreamLoad(DorisStreamLoadImpl streamLoad) {
        this.dorisStreamLoadImpl = streamLoad;
    }

    public boolean isLoading() {
        return this.loading;
    }
}
