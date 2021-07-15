package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.ClickHouseConfig;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager.ClickHouseManager;
import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager.ClickHouseManagerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    public static final String[] PRODUCT_IDS = {"123456", "adfasldfjlsk", "192301udfj", "sifuo3p2wjot23qu0lkfdjupo", "023709uwodfj1", "1lfsjdou2o"};
    public static final String[] EVENT_CODES = {"zxcvbnm", "asdfghjkl", "qwertuio", "poiuytre", "0987vbnm"};

    public static void main(String[] args) throws Exception {
        ClickHouseConfig clickHouseConfig = buildClickhouseConfig(args);
        System.out.println(clickHouseConfig.toString());
        Thread.sleep(5 * 1000);
        ClickHouseManager clickHouseManager = ClickHouseManagerBuilder.build(clickHouseConfig, null);
        System.out.println("start to write.");
        long start = System.currentTimeMillis();
        for (int i=0; i < 20_000_000; i++) {
            clickHouseManager.addRecord("", createRecord());
        }
        long end = System.currentTimeMillis();
        Thread.sleep(10*1000);
        System.out.printf("time cost: " + (end - start) / 1000);
        LOG.info("time cost: " + (end - start) / 1000);
        clickHouseManager.close();
    }

    private static ClickHouseConfig buildClickhouseConfig(String[] args) {
        if (args != null && args.length >= 1) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                ClickHouseConfig config = objectMapper.readValue(args[0], ClickHouseConfig.class);
                return config;
            } catch (Exception e) {
                LOG.info(e.getMessage(), e);
            }
        }
        ClickHouseConfig clickHouseConfig = new ClickHouseConfig();
        clickHouseConfig.setHostAddr("100.110.21.18:12101");
        clickHouseConfig.setClusterName("ch01");
        clickHouseConfig.setDbName("xingpan_demo");
        clickHouseConfig.setTableName("dh_data_test_2021071422_local");
        clickHouseConfig.setUsername("default");
        clickHouseConfig.setPwd("7T6Hba8s");
        clickHouseConfig.setBatchSize(50);
        clickHouseConfig.setBatchInterval(5000);
        return clickHouseConfig;
    }

    private static Map<String, String> createRecord() {
        Map<String, String> map = new LinkedHashMap<>();
        Long timestamp = System.currentTimeMillis();
        int randNum = new Long (timestamp / 10000).intValue();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
        map.put("ds", sdf.format(new Date()));
        map.put("product_id", PRODUCT_IDS[randNum % PRODUCT_IDS.length]);
        map.put("event_code", EVENT_CODES[randNum % EVENT_CODES.length]);
        map.put("event_time", timestamp.toString());
        return map;
    }
}
