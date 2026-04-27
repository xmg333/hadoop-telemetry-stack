package x.mg.metrics.flink.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class BatchInserter {

    @FunctionalInterface
    interface RowBinder<T> {
        void bind(PreparedStatement ps, T row) throws SQLException;
    }

    private final Connection connection;
    private final boolean isClickHouse;

    BatchInserter(Connection connection, boolean isClickHouse) {
        this.connection = connection;
        this.isClickHouse = isClickHouse;
    }

    <T> int executeBatch(String sql, List<T> rows, RowBinder<T> binder) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(sql);
        for (T r : rows) {
            binder.bind(ps, r);
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }
}
