package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal;

import com.tencent.datahub.blade.flink.component.platform.utils.AssertUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.ClickhouseJdbcUrlParser;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

/**
 *
 */
public class RoundRobinPriorityDataSource implements DataSource {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RoundRobinPriorityDataSource.class);
    private static final Pattern URL_TEMPLATE = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "" +
            "//([a-zA-Z0-9_:,.-]+)" +
            "(/[a-zA-Z0-9_]+" +
            "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+)*)?" +
            ")?");
    private final ThreadLocal<Random> randomThreadLocal = new ThreadLocal<Random>();
    private List<String> firstAllUrls;
    private volatile List<String> firstActiveUrls;
    private int firstPriority;
    private final AtomicInteger firstUrlIndex = new AtomicInteger(0);
    private List<String> secondlyAllUrls;
    private volatile List<String> secondlyActiveUrls;
    private int secondlyPriority;
    private final AtomicInteger secondlyUrlIndex = new AtomicInteger(0);
    private int randomBound;
    private final int randomAmplify = 1000;
    private final ClickHouseProperties properties;
    private final ClickHouseDriver driver = new ClickHouseDriver();
    private PrintWriter printWriter;
    private int loginTimeoutSeconds = 0;

    public RoundRobinPriorityDataSource(List<String> firstAllUrls, int firstPriority, List<String> secondlyAllUrls
            , int secondlyPriority, Properties properties) {
        AssertUtil.assertTrue(CollectionUtils.isNotEmpty(firstAllUrls)
                , "empty firstUrls for RoundRobinPriorityDataSource!");
        this.firstAllUrls = new ArrayList<>(firstAllUrls.size()); // firstAllUrls;
        checkThenAddUrls(firstAllUrls, this.firstAllUrls);
        firstActiveUrls = this.firstAllUrls;
        this.firstPriority = firstPriority;

        this.secondlyAllUrls = new ArrayList<>(); //secondlyAllUrls;
        checkThenAddUrls(secondlyAllUrls, this.secondlyAllUrls);
        secondlyActiveUrls = this.secondlyAllUrls;
        this.secondlyPriority = secondlyPriority;
        randomBound = (this.firstPriority + this.secondlyPriority) * randomAmplify;
        try {
            ClickHouseProperties argsProperties = new ClickHouseProperties(properties);
            ClickHouseProperties localProperties =
                    ClickhouseJdbcUrlParser.parse(this.firstAllUrls.get(0), argsProperties.asProperties());
            localProperties.setHost(null);
            localProperties.setPort(-1);

            this.properties = localProperties;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void checkThenAddUrls(List<String> sourceUrls, List<String> targetUrls) {
        for (final String url : sourceUrls) {
            try {
                if (driver.acceptsURL(url)) {
                    targetUrls.add(url);
                } else {
                    log.error("that url is has not correct format: {}", url);
                }
            } catch (SQLException e) {
                throw new IllegalArgumentException("error while checking url: " + url, e);
            }
        }
    }

    public List<String> getFirstAllUrls() {
        return Collections.unmodifiableList(firstAllUrls);
    }

    public List<String> getSecondlyAllUrls() {
        return Collections.unmodifiableList(secondlyAllUrls);
    }

    static List<String> splitUrl(final String url) {
        Matcher m = URL_TEMPLATE.matcher(url);
        if (!m.matches()) {
            throw new IllegalArgumentException("Incorrect url");
        }
        String database = m.group(2);
        if (database == null) {
            database = "";
        }
        String[] hosts = m.group(1).split(",");
        final List<String> result = new ArrayList<String>(hosts.length);
        for (final String host : hosts) {
            result.add(JDBC_CLICKHOUSE_PREFIX + "//" + host + database);
        }
        return result;
    }

    static Properties parseUriQueryPart(String query, Properties defaults) {
        if (query == null) {
            return defaults;
        }
        Properties urlProps = new Properties(defaults);
        String[] queryKeyValues = query.split("&");
        for (String keyValue : queryKeyValues) {
            String[] keyValueTokens = keyValue.split("=");
            if (keyValueTokens.length == 2) {
                urlProps.put(keyValueTokens[0], keyValueTokens[1]);
            } else {
                log.warn("don't know how to handle parameter pair: {}", keyValue);
            }
        }
        return urlProps;
    }

    private boolean ping(final String url) {
        try {
            driver.connect(url, properties).createStatement().execute("SELECT 1");
            return true;
        } catch (Exception e) {
            log.debug("Unable to connect using {}", url, e);
            return false;
        }
    }

    /**
     * Checks if clickhouse on url is alive, if it isn't, add it into firstActiveUrls or secondlyActiveUrls.
     *
     * @return number of avaliable clickhouse urls
     */
    public synchronized int actualize() {
        return firstActiveUrls.size() + secondlyActiveUrls.size();
    }

    private String getNextUrl() throws SQLException {
        Random random = this.randomThreadLocal.get();
        if (random == null) {
            this.randomThreadLocal.set(new Random());
            random = this.randomThreadLocal.get();
        }
        int randomValue = random.nextInt(randomBound);
        if (randomValue < firstPriority * randomAmplify) {
            int index = firstUrlIndex.getAndIncrement();
            if (index >= firstAllUrls.size() * randomAmplify) {
                firstUrlIndex.set(0);
            }
            List<String> activeUrls = firstActiveUrls;
            return activeUrls.get(index % activeUrls.size());
        } else {
            int index = secondlyUrlIndex.getAndIncrement();
            if (index >= secondlyAllUrls.size() * randomAmplify) {
                secondlyUrlIndex.set(0);
            }
            List<String> activeUrls = secondlyActiveUrls;
            return activeUrls.get(index % activeUrls.size());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return driver.connect(getNextUrl(), properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        return driver.connect(getNextUrl(), properties.withCredentials(username, password));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(PrintWriter printWriter) throws SQLException {
        this.printWriter = printWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
//        throw new SQLFeatureNotSupportedException();
        loginTimeoutSeconds = seconds;
    }

    /**
     * {@inheritDoc}
     */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * set time period of removing connections
     *
     * @param rate     value for time unit
     * @param timeUnit time unit for checking
     * @return this datasource with changed settings
     * @see ClickHouseDriver#scheduleConnectionsCleaning
     */
    public RoundRobinPriorityDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit) {
        driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }

    /**
     * set time period for checking availability connections
     *
     * @param delay    value for time unit
     * @param timeUnit time unit for checking
     * @return this datasource with changed settings
     */
    public RoundRobinPriorityDataSource scheduleActualization(int delay, TimeUnit timeUnit) {
        ScheduledConnectionCleaner.INSTANCE.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e) {
                    log.error("Unable to actualize urls", e);
                }
            }
        }, 0, delay, timeUnit);

        return this;
    }

    public ClickHouseProperties getProperties() {
        return properties;
    }

    static class ScheduledConnectionCleaner {
        static final ScheduledExecutorService INSTANCE = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());

        static class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        }
    }
}
