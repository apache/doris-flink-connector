package org.apache.doris.flink.sink.copy;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/** Flink 2.2 specific wrapper for the shared core {@link DorisCopyWriter} implementation. */
public class DorisCopyWriterAdapter<IN>
        implements DorisAbstractWriter<IN, DorisWriterState, DorisCopyCommittable> {

    private final DorisCopyWriter<IN> delegate;

    public DorisCopyWriterAdapter(
            WriterInitContext initContext,
            DorisRecordSerializer<IN> serializer,
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions executionOptions) {

        long restoreCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        int subtaskId = initContext.getTaskInfo().getIndexOfThisSubtask();

        this.delegate =
                new DorisCopyWriter<>(
                        restoreCheckpointId,
                        subtaskId,
                        serializer,
                        dorisOptions,
                        dorisReadOptions,
                        executionOptions);
    }

    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        delegate.write(in, context);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        delegate.flush(endOfInput);
    }

    @Override
    public Collection<DorisCopyCommittable> prepareCommit()
            throws IOException, InterruptedException {
        return delegate.prepareCommit();
    }

    @Override
    public List<DorisWriterState> snapshotState(long checkpointId) throws IOException {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
