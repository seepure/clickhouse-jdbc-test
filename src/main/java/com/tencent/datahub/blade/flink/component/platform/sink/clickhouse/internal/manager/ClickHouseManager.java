package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager;

import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseConfig;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClusterEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClickHouseManager is in charge of:
 * <br> 1. management of connections, including balancing request to diff connection,
 * binding connection to shard, connection failover.
 * <br> 2. sync information of cluster metadata related to given
 * database&table. <br>
 */
public abstract class ClickHouseManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseManager.class);
    protected ClickHouseConfig clickHouseConfig;

    protected volatile Map<String, Map<String, String>> tableSchemas;

    /**
     * <p>The map key is shardNum (begin with 1),</p>
     * <p>the map value is the list of the replicas corresponding to the shard</p>
     */
    protected volatile Map<Integer, List<ClusterEntity>> clusterEntityMap;

    /**
     * datasource for maintaining metadata.
     */
    protected BalancedClickhouseDataSource metaDataSource;

    public ClickHouseManager(ClickHouseConfig clickHouseConfig) {
        this.clickHouseConfig = clickHouseConfig;
        init();
    }

    protected void init() {
        try {
            if (null == clickHouseConfig.getHttpPort()) {
                clickHouseConfig.setHttpPort(8123);
            }
            Properties jdbcProperties = new Properties();
            if (StringUtils.isNotBlank(clickHouseConfig.getUsername())
                    && StringUtils.isNotBlank(clickHouseConfig.getPwd())) {
                jdbcProperties.setProperty(ClickHouseQueryParam.USER.getKey(), clickHouseConfig.getUsername());
                jdbcProperties.setProperty(ClickHouseQueryParam.PASSWORD.getKey(), clickHouseConfig.getPwd());
            }
            metaDataSource = createMetaDataSource(jdbcProperties);
            tableSchemas = loadTableSchemas(clickHouseConfig.getDbName(), clickHouseConfig.getTableName());
            LOG.info("clickhouse tableSchemas is : " + tableSchemas.toString());
            clusterEntityMap = loadClusterShardMap(clickHouseConfig.getClusterName());
            LOG.info("clusterEntityMap is : " + clusterEntityMap.toString());
            initWritersAndDataSource(jdbcProperties);
        } catch (Exception e) {
            LOG.error("create ClickHouseManager instance failed,", e);
            throw new RuntimeException("create ClickHouseManager instance failed,", e);
        }
    }

    protected Map<Integer, List<ClusterEntity>> loadClusterShardMap(String clusterName)
            throws SQLException {
        String sqlPattern = "select cluster, shard_num, replica_num, host_name, host_address, "
                + "port from system.clusters where cluster='%s' ";
        Map<Integer, List<ClusterEntity>> result = new ConcurrentHashMap<>();
        ClickHouseConnection connection = null;
        try {
            connection = getMetaConnection();
            ClickHouseStatement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    String.format(sqlPattern, clusterName)
            );
            while (resultSet.next()) {
                String cluster = resultSet.getString("cluster");
                int shardNum = resultSet.getInt("shard_num");
                int replicaNum = resultSet.getInt("replica_num");
                String hostName = resultSet.getString("host_name");
                String hostAddress = resultSet.getString("host_address");
                int port = resultSet.getInt("port");
                ClusterEntity clusterEntity = new ClusterEntity();
                clusterEntity.setCluster(cluster);
                clusterEntity.setShardNum(shardNum);
                clusterEntity.setReplicaNum(replicaNum);
                clusterEntity.setHostName(hostName);
                clusterEntity.setHostAddress(hostAddress);
                clusterEntity.setPort(port);
                List<ClusterEntity> list = result.get(shardNum);
                if (list == null) {
                    list = new ArrayList<>();
                    result.put(shardNum, list);
                }
                list.add(clusterEntity);
            }
        } catch (Exception e) {
            throw new RuntimeException("getClusterMap failed! " + e.getMessage(), e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        if (result == null || result.isEmpty()) {
            throw new RuntimeException("getClusterMap failed! empty map!");
        }
        return result;
    }

    protected Map<String, Map<String, String>> loadTableSchemas(String database, String table)
            throws SQLException {
        //todo extend this method to support multi-table and db
        Map<String, Map<String, String>> result = new HashMap<>();
        //should use LinkedHashMap to maintain the columns order.
        Map<String, String> tableSchema = new LinkedHashMap<>();
        ClickHouseConnection connection = null;
        try {
            connection = getMetaConnection();
            ClickHouseStatement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    String.format("select name, type from system.columns where "
                            + "database='%s' and table='%s'", database, table)
            );
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String type = resultSet.getString("type");
                tableSchema.put(name, type);
            }
            result.put(table, tableSchema);
        } catch (Exception e) {
            throw new RuntimeException("loadTableSchemas failed!", e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return result;
    }

    protected BalancedClickhouseDataSource createMetaDataSource(Properties properties) {
        StringBuilder connectSb = new StringBuilder();
        String[] urls = null;
        String seedUrl = clickHouseConfig.getHostAddr();
        if (seedUrl.contains(",")) {
            urls = seedUrl.split(",");
        } else if (seedUrl.contains(";")) {
            urls = seedUrl.split(";");
        } else {
            urls = new String[1];
            urls[0] = seedUrl;
        }
        if (urls == null || urls.length < 1) {
            throw new IllegalArgumentException("seed_url format error, the format should be: "
                    + "ip1:port1,ip2:port2 or ip1:port1;ip2:port2");
        }
        connectSb.append("jdbc:clickhouse://");
        for (int i = 0; i < urls.length; i++) {
            String url = urls[i];
            connectSb.append(url);
            if (i != urls.length - 1) {
                connectSb.append(",");
            }
        }
        connectSb.append("/").append(clickHouseConfig.getDbName());

        // add extra props of clickhouse datasource
        //properties.setProperty("compress", "true");
        BalancedClickhouseDataSource balancedClickhouseDataSource =
                new BalancedClickhouseDataSource(
                        connectSb.toString(),
                        properties);
        //balancedClickhouseDataSource.scheduleActualization(3, TimeUnit.SECONDS);
        return balancedClickhouseDataSource;
    }

    protected List<ClusterEntity> getClusterEntityList() {
        if (clusterEntityMap != null && !clusterEntityMap.isEmpty()) {
            List<ClusterEntity> list = new ArrayList<>();
            for (List<ClusterEntity> value : clusterEntityMap.values()) {
                if (CollectionUtils.isNotEmpty(value)) {
                    list.addAll(value);
                }
            }
            return Collections.unmodifiableList(list);
        }
        return Collections.unmodifiableList(new ArrayList<>());
    }

    protected List<String> getClickHouseConnectionUrls(List<ClusterEntity> clusterEntityList) {
        if (CollectionUtils.isEmpty(clusterEntityList)) {
            return Collections.emptyList();
        }
        List<String> urls = new ArrayList<>();
        for (ClusterEntity clusterEntity : clusterEntityList) {
            StringBuilder connectSb = new StringBuilder();
            connectSb.append("jdbc:clickhouse://");
            //因为官方jdbc使用的是http port, 所以用8123端口
            connectSb.append(clusterEntity.getHostAddress())
                    .append(":")
                    .append(clickHouseConfig.getHttpPort())
                    .append("/").append(clickHouseConfig.getDbName());
            urls.add(connectSb.toString());
        }
        return urls;
    }

    protected ClickHouseConnection getMetaConnection() throws SQLException {
        return metaDataSource.getConnection();
    }

    protected abstract void initWritersAndDataSource(Properties properties);

    public Map<String, String> getTableSchema(String table) {
        return tableSchemas.get(table);
    }

    public ClickHouseConfig getClickHouseConfig() {
        return clickHouseConfig;
    }

    public abstract ClickHouseConnection getConnection() throws SQLException;

    public abstract ClickHouseConnection getConnection(int writerId) throws SQLException;

    public abstract void addRecord(String key, Map<String, String> msg);

    /**
     * Flush data of all writers to clickhouse.
     * <br> If using checkpoint in flink, this method should be invoked by
     * {@link CheckpointedFunction#snapshotState(FunctionSnapshotContext)},
     * <br> Otherwise, let writers control flushing.
     */
    public abstract void flush();

    public abstract boolean isClosed();

    public abstract void restoreBuffer(Object stateContext);

    public abstract void snapshotBuffer(Object stateContext);
}
