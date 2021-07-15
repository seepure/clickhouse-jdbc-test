package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager;

import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseConfig;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.FlinkRuntimeParam;

import java.util.Objects;

public class ClickHouseManagerBuilder {

    public static ClickHouseManager build(ClickHouseConfig clickHouseConfig, FlinkRuntimeParam flinkRuntimeParam) {
        if ("cloud".equalsIgnoreCase(clickHouseConfig.getClusterType())
            || Objects.equals(true, clickHouseConfig.isDistribute())) {
            return new SimpleClickHouseManager(clickHouseConfig);
        }
        return new ClickHouseBalancedManager(clickHouseConfig);
    }
}
