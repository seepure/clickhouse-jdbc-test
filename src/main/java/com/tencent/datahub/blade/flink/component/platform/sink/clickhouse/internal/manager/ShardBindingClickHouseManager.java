package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager;

import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseConfig;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseWriter;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClusterEntity;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.FlinkRuntimeParam;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.RoundRobinPriorityDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * When writer of FlinkTpgAwareClickHouseManager writing data,
 * <p>it will try to connect to one specific shard at first.ifnon-replicas are available,the writer
 * will randomly choose one of the available replica of other shards and write data to it.
 * <br> FlinkTpgAwareClickHouseManager will bind the flink sink subTask which holding the
 * FlinkTpgAwareClickHouseManager's instance to only a shard of ClickHouse. <br>
 */
public class ShardBindingClickHouseManager extends ClickHouseManager {

    private static final Logger LOG = LoggerFactory
            .getLogger(ShardBindingClickHouseManager.class);

    private volatile boolean isClosed = false;

    /**
     * (The index of the parallel sink subtask), mapping to the shardNum of ClickHouse.
     */
    private int subTaskIndex;

    /**
     * The parallelism with which the sink task runs.
     */
    private int numberOfSubTasks;

    private ClickHouseWriter writer;

    /**
     * dataSource corresponding to one specific shard.
     */
    private RoundRobinPriorityDataSource dataSource;

    //backupDatasource

    public ShardBindingClickHouseManager(ClickHouseConfig clickHouseConfig, FlinkRuntimeParam flinkRuntimeParam) {
        super(clickHouseConfig);
        this.subTaskIndex = flinkRuntimeParam.getIndexOfSubtask();
        this.numberOfSubTasks = flinkRuntimeParam.getParallelism();
    }

    @Override
    protected void initWritersAndDataSource(Properties properties) {
        //sink的subTask的数量应该是clickhouse cluster shard的整数倍
        //todo 如果sink的subTask的数量不是shard的整数倍或者多或者少怎么办?
        int times = numberOfSubTasks / clusterEntityMap.size();
        int mod = numberOfSubTasks % clusterEntityMap.size();
        int antiTimes = clusterEntityMap.size() / numberOfSubTasks;
        int antiMod = clusterEntityMap.size() % numberOfSubTasks;

        List<ClusterEntity> firstClusterEntities = new ArrayList<>();
        List<ClusterEntity> secondClusterEntities = new ArrayList<>();
        int firstPriority = -1;
        int secondlyPriority = -1;
        if (times < 1) { //case 1. p=7, s=10 ; case 2. p=7, s=50
            for (int i = 0; i < antiTimes; i++) {
                int currentIndex = subTaskIndex % clusterEntityMap.size() + 1 + i * numberOfSubTasks;
                firstClusterEntities.addAll(clusterEntityMap.get(currentIndex));
            }
            if (antiMod > 0) {
                for (int i = numberOfSubTasks * antiTimes + 1;  i <= clusterEntityMap.size();  i++) {
                    secondClusterEntities.addAll(clusterEntityMap.get(i));
                }
                Collections.sort(secondClusterEntities, new ClickhouseEntityComparator());
            }
            firstPriority = numberOfSubTasks * antiTimes;
            secondlyPriority = clusterEntityMap.size() - firstPriority;
        } else {    //case 1. p=10, s=10; case2. p=13, s=10; case 3. p=71, s=23
            if (subTaskIndex >= clusterEntityMap.size() * times) {
                for (Map.Entry<Integer, List<ClusterEntity>> entityEntry : clusterEntityMap.entrySet()) {
                    firstClusterEntities.addAll(entityEntry.getValue());
                }
            } else {
                int currentIndex = subTaskIndex % clusterEntityMap.size() + 1;
                firstClusterEntities.addAll(clusterEntityMap.get(currentIndex));
            }
            firstPriority = 1;
            secondlyPriority = 0;
        }
        Collections.sort(firstClusterEntities, new ClickhouseEntityComparator());
        dataSource = new RoundRobinPriorityDataSource(
                getClickHouseConnectionUrls(firstClusterEntities), firstPriority
                , getClickHouseConnectionUrls(secondClusterEntities), secondlyPriority, properties);

        writer = new ClickHouseWriter(1, this);
        LOG.info(String.format("FlinkTpgAwareClickHouseManager initialization finished. "
                        + "subTaskIndex: %d, dataSource first URLS: %s, dataSource second URLS: %s .",
                subTaskIndex, dataSource.getFirstAllUrls().toString(), dataSource.getSecondlyAllUrls().toString()));
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
        writer.addRecord(msg);
    }

    @Override
    public void flush() {
        writer.flush();
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
        writer.close();
    }

    static class ClickhouseEntityComparator implements Comparator<ClusterEntity> {

        @Override
        public int compare(ClusterEntity o1, ClusterEntity o2) {
            if (o1.getReplicaNum() == o2.getReplicaNum()) {
                return o1.getShardNum() - o2.getShardNum();
            }
            return o1.getReplicaNum() - o2.getReplicaNum();
        }
    }
}

