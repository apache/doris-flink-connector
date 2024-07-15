package org.apache.doris.flink.flight.arrow;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.flight.split.DorisFlightSourceSplit;
import org.apache.doris.flink.flight.split.DorisFlightSourceSplitState;

import java.util.List;
import java.util.Map;

/** A {@link SourceReader} that read records from {@link DorisFlightSourceSplit}. */
public class DorisFlightSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                List, T, DorisFlightSourceSplit, DorisFlightSourceSplitState> {

    public DorisFlightSourceReader(
            DorisOptions options,
            DorisReadOptions readOptions,
            RecordEmitter<List, T, DorisFlightSourceSplitState> recordEmitter,
            SourceReaderContext context,
            Configuration config) {
        super(
                () -> new DorisFlightSourceSplitReader(options, readOptions),
                recordEmitter,
                config,
                context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, DorisFlightSourceSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected DorisFlightSourceSplitState initializedState(DorisFlightSourceSplit split) {
        return new DorisFlightSourceSplitState(split);
    }

    @Override
    protected DorisFlightSourceSplit toSplitType(
            String splitId, DorisFlightSourceSplitState splitState) {
        return splitState.toDorisSourceSplit();
    }
}
