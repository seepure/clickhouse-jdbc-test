package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager;

import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseConfig;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseWriter;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClusterEntity;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.Constant;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * As name said, ClickHouseBalancedManager
 * <p>use all enable urls of the ClickHouse cluster to build the dataSource.</p>
 */
public class ClickHouseBalancedManager extends ClickHouseManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBalancedManager.class);
    private volatile boolean isClosed = false;
    private Random random = new Random();
    private BalancedClickhouseDataSource dataSource;

    private int writerNum;  //todo dynamic adapted writer number
    private List<ClickHouseWriter> writerList;

    public ClickHouseBalancedManager(ClickHouseConfig clickHouseConfig) {
        super(clickHouseConfig);
    }

    @Override
    protected void initWritersAndDataSource(Properties properties) {
        writerNum = Integer.parseInt(properties.getProperty(Constant.WRITER_NUMBER, "1"));
        dataSource = createBalancedDataSource(getClusterEntityList(), properties);
        if (writerNum > clusterEntityMap.size()) {
            writerNum = clusterEntityMap.size();
        }
        writerList = new ArrayList<>(writerNum);
        for (int i = 0; i < writerNum; i++) {
            ClickHouseWriter writer = new ClickHouseWriter(i + 1, this);
            writerList.add(writer);
        }
        LOG.info("ClickHouseBalancedManager initialization finished. dataSource url: "
                + dataSource.getAllClickhouseUrls().toString());
    }

    private BalancedClickhouseDataSource createBalancedDataSource(List<ClusterEntity> clusterEntityList,
                                                                    Properties propsDataSource) {
        if (CollectionUtils.isEmpty(clusterEntityList)) {
            throw new RuntimeException("createDataSource error, "
                    + "caused by empty clusterEntityList");
        }
        StringBuilder connectSb = new StringBuilder();
        connectSb.append("jdbc:clickhouse://");
        for (ClusterEntity clusterEntity : clusterEntityList) {
            //因为官方jdbc使用的是http port, 所以用8123端口
            connectSb.append(clusterEntity.getHostAddress())
                    .append(":")
                    .append(clickHouseConfig.getHttpPort())
                    .append(",");
        }
        String connectString = connectSb.toString();
        connectString = connectString.substring(0, connectString.length() - 1);
        connectString = connectString + "/" + clickHouseConfig.getDbName();
        BalancedClickhouseDataSource dataSource =
                new BalancedClickhouseDataSource(connectString, propsDataSource);
        dataSource.scheduleActualization(3, TimeUnit.SECONDS);
        return dataSource;
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public ClickHouseConnection getConnection(int writerId) throws SQLException {
        return getConnection();
    }

    @Override
    public void addRecord(String key, Map<String, String> msg) {
        int index = key != null ? key.hashCode() % writerNum : random.nextInt(writerNum);
        writerList.get(index).addRecord(msg);
    }

    @Override
    public void flush() {
        for (ClickHouseWriter writer : writerList) {
            writer.flush();
        }
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
        isClosed = true;
        for (ClickHouseWriter clickhouseWriter : writerList) {
            clickhouseWriter.close();
        }
    }

}
