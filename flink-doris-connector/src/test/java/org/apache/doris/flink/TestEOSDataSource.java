package org.apache.doris.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class TestEOSDataSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    protected static final Logger LOG = LoggerFactory.getLogger(TestEOSDataSource.class);

    private transient ListState<Long> checkpointedState;
    private Long id = 0L;
    private int batchSize;
    public TestEOSDataSource(int batchSize) {
        this.batchSize = batchSize;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<String>  sourceContext) throws Exception {
        int taskId = getRuntimeContext().getIndexOfThisSubtask() + 1;
        while (true){
            for(int i=0;i<batchSize;i++){
                id = id+1;
                Map<String,Object> json = new HashMap<>();
                json.put("id",id);
                json.put("task_id",taskId);
                json.put("name", UUID.randomUUID().toString());
                sourceContext.collect(new ObjectMapper().writeValueAsString(json));
            }
            LOG.info("id:{},state:{}",id,checkpointedState.get());;
            Thread.sleep(5000l);
        }
    }

    @Override
    public void cancel() {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        checkpointedState.add(1234567890L);
        LOG.info("====checkpoint====={}",checkpointedState.get());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointedState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("id", TypeInformation.of(Long.class)));
        LOG.info("id:====={}",id);
        if(context.isRestored()){
            Iterator<Long> iterator = checkpointedState.get().iterator();
            while (iterator.hasNext()){
                id += iterator.next();
            }
            LOG.info(
                    "Consumer subtask {} restored state: {}.id:{}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    checkpointedState,id);
        }else{
            LOG.info(
                    "Consumer subtask {} has no restore state.",
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}