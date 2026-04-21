package x.mg.metrics.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import x.mg.metrics.flink.model.*;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FlinkCategoryJdbcSink extends RichSinkFunction<WideRowAccumulator.FlushResult> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(FlinkCategoryJdbcSink.class.getName());

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final int batchSize;
    private final long flushIntervalMs;
    private final boolean isClickHouse;

    private transient CategoryJdbcSink delegate;

    public FlinkCategoryJdbcSink(String jdbcUrl, String jdbcUser, String jdbcPassword,
                                  int batchSize, long flushIntervalMs, boolean isClickHouse) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.isClickHouse = isClickHouse;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        delegate = new CategoryJdbcSink(jdbcUrl, jdbcUser, jdbcPassword, batchSize, flushIntervalMs, isClickHouse);
        delegate.open();
        LOG.info("FlinkCategoryJdbcSink opened (" + (isClickHouse ? "clickhouse" : "mysql") + "): " + jdbcUrl);
    }

    @Override
    public void invoke(WideRowAccumulator.FlushResult result, Context context) throws Exception {
        delegate.writeFlushResult(result);
    }

    @Override
    public void close() throws Exception {
        if (delegate != null) {
            delegate.close();
        }
    }
}
