package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager;

import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseConfig;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseWriter;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class SimpleClickHouseManager extends ClickHouseManager {

    private final ClickHouseDriver driver = new ClickHouseDriver();
    private volatile boolean isClosed = false;
    private String jdbcUrl;
    private Properties jdbcProperties;
    private ClickHouseWriter writer;

    public SimpleClickHouseManager(ClickHouseConfig clickHouseConfig) {
        super(clickHouseConfig);
    }

    @Override
    protected void initWritersAndDataSource(Properties properties) {
        jdbcUrl = JDBC_CLICKHOUSE_PREFIX + "//" + clickHouseConfig.getHostAddr() + "/" + clickHouseConfig.getDbName();
        jdbcProperties = new Properties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            jdbcProperties.put(entry.getKey(), entry.getValue());
        }
        writer = new ClickHouseWriter(0, this);
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return driver.connect(jdbcUrl, jdbcProperties);
    }

    @Override
    public ClickHouseConnection getConnection(int writerId) throws SQLException {
        return driver.connect(jdbcUrl, jdbcProperties);
    }

    @Override
    public void addRecord(String key, Map<String, String> msg) {
        writer.addRecord(msg);
    }

    @Override
    public void flush() {

    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void restoreBuffer(Object stateContext) {

    }

    @Override
    public void snapshotBuffer(Object stateContext) {

    }

    @Override
    public void close() throws Exception {
        isClosed = false;
    }

}
