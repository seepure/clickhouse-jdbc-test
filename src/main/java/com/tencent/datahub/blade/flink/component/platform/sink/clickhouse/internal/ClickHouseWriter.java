package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal;

import com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal.manager.ClickHouseManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ClickHouseWriter is in charge of data buffering, data flushing.
 * <p>If data flushing failed for given retry times, it
 * will throw exception to make the job stop.</p>
 */
public class ClickHouseWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseWriter.class);

    private volatile boolean isClosed = false;

    //id is used to bind special shard. begin with 1.
    private final int id;

    private final ClickHouseManager manager;

    private final BlockingQueue<String> buffer;

    private volatile long lastFlushTime;

    private final String tableName;

    private final int batchSize;

    private final long flushInterval;

    //private Thread checkFlushThread;
    private final ScheduledExecutorService scheduledExecutorService;

    private final ScheduledExecutorService flushScheduledExecutor;

    private final ReentrantLock flushLock;

    private final ExecutorService flushExecutorService;

    // not thread-safe = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final ThreadLocal<SimpleDateFormat> threadLocal;

    private volatile Map<String, String> logMsgMap = null;

    public ClickHouseWriter(int id, ClickHouseManager manager) {
        this.id = id;
        this.manager = manager;
        ClickHouseConfig clickHouseConfig = manager.getClickHouseConfig();
        flushInterval = clickHouseConfig.getBatchInterval() != null ? clickHouseConfig.getBatchInterval() : 5000;
        batchSize = clickHouseConfig.getBatchSize() != null ? clickHouseConfig.getBatchSize() : 60000;
        tableName = clickHouseConfig.getTableName();
        buffer = new LinkedBlockingQueue<>(batchSize + batchSize / 3);
        lastFlushTime = System.currentTimeMillis();
        threadLocal = ThreadLocal.withInitial(() ->
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        flushLock = new ReentrantLock();
        flushExecutorService = Executors.newFixedThreadPool(2);
        flushScheduledExecutor = Executors.newScheduledThreadPool(1);
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        flushScheduledExecutor.scheduleWithFixedDelay(() -> {
            if (checkSize() || checkTime()) {
                flushAsync();
            }
        }, 100, 200, TimeUnit.MILLISECONDS);

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            // for monitor
            LOG.info(String.format("time: %s, buffer size: %d", System.currentTimeMillis(),
                    buffer.size()));
            if (System.currentTimeMillis() % 3 == 0 && logMsgMap != null) {
                LOG.info("logMsgMap: " + logMsgMap.toString());
                logMsgMap = null;
            }
        }, 100, flushInterval, TimeUnit.MILLISECONDS);
    }


    @Override
    public void close() throws Exception {
        isClosed = true;
        if (flushScheduledExecutor != null) {
            flushScheduledExecutor.shutdown();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
        Thread.sleep(500);
        try {
            flush();
        } catch (Exception e) {
        }
        if (threadLocal != null) {
            threadLocal.remove();
        }
    }

    public void addRecord(Map<String, String> msg) {
        try {
            if (!isClosed && msg != null && msg.size() > 0) {
                //todo 增加时间分区字段配置以及时间过滤规则配置

                StringBuilder sb = new StringBuilder();
                Map<String, String> tableSchema = manager.getTableSchema(tableName);
                int i = 0;
                for (Map.Entry<String, String> column : tableSchema.entrySet()) {
                    String columnName = column.getKey();
                    String columnValue = msg.get(columnName);
                    if (StringUtils.isNotBlank(columnValue)) {
                        sb.append(columnName).append(Constant.EQ_EXP).append(columnValue);
                        if (i < tableSchema.size() - 1) {
                            sb.append(Constant.SINK_MSG_DELIMITER);
                        }
                    }
                    i++;
                }
                buffer.put(sb.toString());
            }
        } catch (Exception e) {
            //swallow
            LOG.warn(e.getMessage(), e);
        }
    }

    private boolean checkSize() {
        return buffer.size() >= batchSize;
    }

    private boolean checkTime() {
        long currentTime = System.currentTimeMillis();
        return currentTime - lastFlushTime > flushInterval;
    }

    public void flushAsync() {
        flushExecutorService.execute(() -> {
            boolean locked = false;
            try {
                locked = flushLock.tryLock(flushInterval, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
            if (locked) {
                try {
                    if (checkSize() || checkTime()) {
                        flush();
                    }
                } finally {
                    flushLock.unlock();
                }
            }
        });
    }

    public void flush() {
        long lastFlushTime = System.currentTimeMillis();
        try {
            int size = (buffer.size() < batchSize) ? buffer.size() : batchSize;
            if (size == 0) {
                return;
            }
            LOG.info("start flush");
            long startTime = lastFlushTime;
            List<String> list = new ArrayList<>(size);
            buffer.drainTo(list, size);
            String insertSql = buildInsertSql();
            ClickHouseException clickHouseException = null;
            for (int n = 0; n < 2; n++) {
                try {
                    ClickHouseConnection connection = manager.getConnection(id);
                    ClickHouseStatement statement = connection.createStatement();
                    statement.write().send(insertSql, new ClickHouseStreamCallback() {
                        @Override
                        public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                            writeBatch(list, stream);
                        }
                    }, ClickHouseFormat.RowBinary);
                    long endTime = System.currentTimeMillis();
                    LOG.info((String.format("flushed %d message(s) to ClickHouseRowBinaryStream, "
                            + "elapsed: %d ms", list.size(), (endTime - startTime))));
                    clickHouseException = null;
                    break;
                } catch (ClickHouseException ce) {
                    //LOG.error(ce.getMessage(), ce);
                    clickHouseException = ce;
                    Thread.sleep(flushInterval);
                }
            }
            if (clickHouseException != null) {
                clickHouseException.printStackTrace();
                LOG.error(clickHouseException.getMessage(), clickHouseException);
            }
        } catch (Exception e) {

        } finally {
            this.lastFlushTime = lastFlushTime;
        }
    }

    private String buildInsertSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName);
        return sb.toString();
    }

    private Map<String, String> msgToMap(String msg) {
        if (StringUtils.isEmpty(msg)) {
            return null;
        }
        Map<String, String> map = new HashMap<>();
        try {
            String delimiter = Constant.SINK_MSG_DELIMITER;
            String message = msg;
            if (StringUtils.isBlank(message)) {
                return map;
            }
            String[] keyValues = StringUtils.split(message, delimiter);
            if (keyValues == null) {
                return map;
            }
            for (String keyValue : keyValues) {
                int index = keyValue.indexOf("=");
                if (StringUtils.isNotBlank(keyValue) && index > 0) {
                    String key = StringUtils.substring(keyValue, 0, index);
                    if (index == (keyValue.length() - 1)) {
                        map.put(key, "");
                    } else {
                        map.put(key, StringUtils.substring(keyValue, index + 1));
                    }
                }
            }

        } catch (Exception e) {
            //swallow
        }
        return map;
    }

    public void writeBatch(List<String> list, ClickHouseRowBinaryStream stream) {
        for (String msg : list) {
            try {
                Map<String, String> tableSchema = manager.getTableSchema(tableName);
                Map<String, String> msgMap = msgToMap(msg);
                if (tableSchema == null || msgMap == null || msgMap.size() == 0) {
                    continue;
                }
                if (logMsgMap == null) {
                    logMsgMap = msgMap;
                }
                for (Map.Entry<String, String> entry : tableSchema.entrySet()) {
                    String columnName = entry.getKey();
                    String columnValue = msgMap.get(columnName);
                    String columnType = entry.getValue();
                    writeMsgToStream(stream, columnValue, columnType);
                }
            } catch (Exception e) {
                LOG.warn("write one of the batch msg error, oringinal_msg: " + msg, e);
            }
        }
    }

    //todo nullable的设置方法, 默认值的设置讨论
    private void writeMsgToStream(ClickHouseRowBinaryStream stream, String columnValue, String columnType) {

        if (columnType.contains("String") || columnType.contains("string")) {
            if (columnType.contains("Nullable")) {
                writeStringNullable(stream, columnValue);
            } else {
                writeString(stream, columnValue);
            }
        } else if (columnType.contains("Int16") || columnType.contains("int16")
                || columnType.contains("Uint16") || columnType.contains("uint16")) {
            if (columnType.contains("Nullable")) {
                writeInt16Nullable(stream, columnValue);
            } else {
                writeInt16(stream, columnValue);
            }
        } else if (columnType.contains("Int32") || columnType.contains("int32")
                || columnType.contains("Uint32") || columnType.contains("uint32")) {
            if (columnType.contains("Nullable")) {
                writeInt32Nullable(stream, columnValue);
            } else {
                writeInt32(stream, columnValue);
            }
        } else if (columnType.contains("Int64") || columnType.contains("int64")
                || columnType.contains("Uint64") || columnType.contains("uint64")
                || columnType.contains("BIGINT")) {
            if (columnType.contains("Nullable")) {
                writeLongNullable(stream, columnValue);
            } else {
                writeLong(stream, columnValue);
            }
        } else if ("float32".equalsIgnoreCase(columnType)) {
            if (columnType.contains("Nullable")) {
                writeFloatNullable(stream, columnValue);
            } else {
                writeFloat(stream, columnValue);
            }
        } else if ("float64".equalsIgnoreCase(columnType)) {
            if (columnType.contains("Nullable")) {
                writeDoubleNullable(stream, columnValue);
            } else {
                writeDouble(stream, columnValue);
            }
        } else if (columnType.contains("DateTime")) {
            if (columnType.contains("Nullable")) {
                writeDateTimeNullable(stream, columnValue);
            } else {
                writeDateTime(stream, columnValue);
            }
        } else {
            //throw new RuntimeException not support type
        }
    }

    private void writeString(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                stream.writeString(columnValue);
            } else {
                stream.writeString("NULL");
            }
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeStringNullable(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                stream.markNextNullable(false);
                stream.writeString(columnValue);
            } else {
                stream.markNextNullable(true);
            }
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeInt16(ClickHouseRowBinaryStream stream, String columnValue) {
        short value = 0;
        try {
            value = columnValue == null ? value : Short.parseShort(columnValue);
        } catch (NumberFormatException e) {
            //swallow
        }
        try {
            stream.writeInt16(value);
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeInt16Nullable(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                short value = Short.parseShort(columnValue);
                stream.markNextNullable(false);
                stream.writeInt16(value);
            } else {
                stream.markNextNullable(true);
            }
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeInt32(ClickHouseRowBinaryStream stream, String columnValue) {
        int value = 0;
        try {
            value = columnValue == null ? value : Integer.parseInt(columnValue);
        } catch (NumberFormatException e) {
            //swallow
        }
        try {
            stream.writeInt32(value);
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeInt32Nullable(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                int value = Integer.parseInt(columnValue);
                stream.markNextNullable(false);
                stream.writeInt32(value);
            } else {
                stream.markNextNullable(true);
            }
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeLong(ClickHouseRowBinaryStream stream, String columnValue) {
        long value = 0;
        try {
            value = columnValue == null ? value : Long.parseLong(columnValue);
        } catch (NumberFormatException e) {
            //swallow
        }
        try {
            stream.writeInt64(value);
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeLongNullable(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                long value = Long.parseLong(columnValue);
                stream.markNextNullable(false);
                stream.writeInt64(value);
            } else {
                stream.markNextNullable(true);
            }
        } catch (Exception e) {
            //swallow
        }
    }


    private void writeDouble(ClickHouseRowBinaryStream stream, String columnValue) {
        double value = 0;
        try {
            value = columnValue == null ? value : Double.parseDouble(columnValue);
        } catch (NumberFormatException e) {
            //swallow
        }
        try {
            stream.writeFloat64(value);
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeDoubleNullable(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                double value = Double.parseDouble(columnValue);
                stream.markNextNullable(false);
                stream.writeFloat64(value);
            } else {
                stream.markNextNullable(true);
            }
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeFloat(ClickHouseRowBinaryStream stream, String columnValue) {
        float value = 0;
        try {
            value = columnValue == null ? value : Float.parseFloat(columnValue);
        } catch (NumberFormatException e) {
            //swallow
        }
        try {
            stream.writeFloat32(value);
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeFloatNullable(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                float value = Float.parseFloat(columnValue);
                stream.markNextNullable(false);
                stream.writeFloat32(value);
            } else {
                stream.markNextNullable(true);
            }
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeDateTime(ClickHouseRowBinaryStream stream, String columnValue) {
        Date value = new Date();
        try {
            value = columnValue == null ? value : threadLocal.get().parse(columnValue);
        } catch (NumberFormatException | ParseException e) {
            //swallow
        }
        try {
            stream.writeDateTime(value);
        } catch (Exception e) {
            //swallow
        }
    }

    private void writeDateTimeNullable(ClickHouseRowBinaryStream stream, String columnValue) {
        try {
            if (columnValue != null) {
                Date value = threadLocal.get().parse(columnValue);
                stream.markNextNullable(false);
                stream.writeDateTime(value);
            } else {
                stream.markNextNullable(true);
            }
        } catch (Exception e) {
            //swallow
        }
    }
}
