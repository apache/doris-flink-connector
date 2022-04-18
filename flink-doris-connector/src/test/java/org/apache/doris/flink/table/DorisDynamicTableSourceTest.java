package org.apache.doris.flink.table;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.enumerator.PendingSplitsCheckpoint;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.utils.FactoryMocks;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class DorisDynamicTableSourceTest {

    @Test
    public void testDorisUseNewApi() {
        DorisReadOptions.Builder builder = OptionUtils.dorisReadOptionsBuilder();
        builder.setUseOldApi(false);
        final DorisDynamicTableSource actualDorisSource = new DorisDynamicTableSource(OptionUtils.buildDorisOptions(), builder.build(), TableSchema.fromResolvedSchema(FactoryMocks.SCHEMA));
        ScanTableSource.ScanRuntimeProvider provider =
                actualDorisSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertDorisSource(provider);
    }

    @Test
    public void testDorisUseNewApiDefault() {
        final DorisDynamicTableSource actualDorisSource = new DorisDynamicTableSource(OptionUtils.buildDorisOptions(), OptionUtils.buildDorisReadOptions(), TableSchema.fromResolvedSchema(FactoryMocks.SCHEMA));
        ScanTableSource.ScanRuntimeProvider provider =
                actualDorisSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertDorisSource(provider);
    }

    @Test
    public void testDorisUseOldApi() {
        DorisReadOptions.Builder builder = OptionUtils.dorisReadOptionsBuilder();
        builder.setUseOldApi(true);
        final DorisDynamicTableSource actualDorisSource = new DorisDynamicTableSource(OptionUtils.buildDorisOptions(), builder.build(), TableSchema.fromResolvedSchema(FactoryMocks.SCHEMA));
        ScanTableSource.ScanRuntimeProvider provider =
                actualDorisSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertDorisInputFormat(provider);
    }


    private void assertDorisInputFormat(ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider, instanceOf(InputFormatProvider.class));
        final InputFormatProvider inputFormatProvider = (InputFormatProvider) provider;

        InputFormat<RowData, DorisTableInputSplit> inputFormat = (InputFormat<RowData, DorisTableInputSplit>) inputFormatProvider.createInputFormat();
        assertThat(inputFormat, instanceOf(DorisRowDataInputFormat.class));
    }


    private void assertDorisSource(ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider, instanceOf(SourceProvider.class));
        final SourceProvider sourceProvider = (SourceProvider) provider;

        Source<RowData, DorisSourceSplit, PendingSplitsCheckpoint> source =
                (Source<RowData, DorisSourceSplit, PendingSplitsCheckpoint>) sourceProvider.createSource();
        assertThat(source, instanceOf(DorisSource.class));
    }
}
