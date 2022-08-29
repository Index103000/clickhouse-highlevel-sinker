package com.xlvchao.clickhouse.component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.xlvchao.clickhouse.model.ClickHouseSettings;
import com.xlvchao.clickhouse.model.ClickHouseSinkRequest;
import com.xlvchao.clickhouse.util.DateTimeUtil;
import com.xlvchao.clickhouse.util.FutureUtil;
import com.xlvchao.clickhouse.util.TableUtil;
import com.xlvchao.clickhouse.util.ThreadUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClickHouseWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseWriter.class);
    private ExecutorService submitService;
    private List<WriterTask> writeTasks;
    private final BlockingQueue<ClickHouseSinkRequest> commonQueue;
    private final AtomicLong queueCounter = new AtomicLong();
    private final List<DataSource> dataSources = Collections.synchronizedList(new ArrayList<>());
    private final List<CompletableFuture<Boolean>> futures;
    private final ClickHouseSettings clickHouseSettings;
    private static final String PREFIX = "jdbc:clickhouse://";
    private static final String SUFFIX = "?socket_timeout=3600000&max_execution_time=3600";


    public ClickHouseWriter(int threadNum, Properties properties, List<CompletableFuture<Boolean>> futures) {
        this.clickHouseSettings = new ClickHouseSettings(properties);
        this.futures = futures;
        this.commonQueue = new LinkedBlockingQueue<>(clickHouseSettings.getQueueMaxCapacity());
        initHikariDataSource(properties);
        buildWriters(threadNum);
    }

    public void initHikariDataSource(Properties properties) {
        try {
            Preconditions.checkNotNull(properties);

            String addresses = properties.getProperty("clickhouse.hikari.addresses");
            String username = properties.getProperty("clickhouse.hikari.username");
            String password = properties.getProperty("clickhouse.hikari.password");
            String minimumIdle = properties.getProperty("clickhouse.hikari.minimumIdle", "1");
            String maximumPoolSize = properties.getProperty("clickhouse.hikari.maximumPoolSize", "5");

            Preconditions.checkNotNull(addresses);
            Preconditions.checkNotNull(username);
            Preconditions.checkNotNull(password);

            String [] ipPorts =  addresses.split(",");
            for(String ipPort : ipPorts) {
                Properties prop = new Properties();
                prop.put("jdbcUrl", PREFIX.concat(ipPort).concat(SUFFIX));
                prop.put("username", username);
                prop.put("password", password);
                prop.put("minimumIdle", minimumIdle);
                prop.put("maximumPoolSize", maximumPoolSize);
                prop.put("poolName", "aiops-hikari-ds-" + ipPort.substring(0, ipPort.indexOf(":")));
                dataSources.add(genHikariDataSource(prop));
            }
        } catch (Exception e) {
            logger.error("Init Hikari DataSource For ClickHouse Error!", e);
        }
    }

    public HikariDataSource genHikariDataSource(Properties properties) {
        // 连接池配置
        HikariConfig config = new HikariConfig();
        //连接池名
        config.setPoolName(properties.getProperty("poolName"));
        // 最小空闲连接数
        config.setMinimumIdle(Integer.parseInt(properties.getProperty("minimumIdle")));
        // 最大连接数
        config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("maximumPoolSize")));
        // 此属性控制从池中拿到的连接的默认自动提交行为，默认值：true
        config.setAutoCommit(false);
        // 空闲连接存活最大时间
        config.setIdleTimeout(MINUTES.toMillis(10));
        // 池中连接的最长存活时间，值0表示无限存活时间
        config.setMaxLifetime(MINUTES.toMillis(30));
        // 数据库连接超时时间，默认30秒
        config.setConnectionTimeout(SECONDS.toMillis(30));
        //测试链接是否可用的sql语句
        config.setConnectionTestQuery("SELECT 1");
        //账号设置
        config.setJdbcUrl(properties.getProperty("jdbcUrl"));
        config.setUsername(properties.getProperty("username"));
        config.setPassword(properties.getProperty("password"));
        return new HikariDataSource(config);
    }


    private void buildWriters(int threadNum) {
        try {
            logger.info("Building writers...");

            ThreadFactory submitServiceFactory = ThreadUtil.threadFactory("clickhouse-writer");
            submitService = Executors.newFixedThreadPool(threadNum, submitServiceFactory);

            writeTasks = Lists.newArrayListWithCapacity(threadNum);
            for (int i = 0; i < threadNum; i++) {
                WriterTask task = new WriterTask(i, dataSources, commonQueue, clickHouseSettings, futures, queueCounter);
                writeTasks.add(task);
                submitService.submit(task);
            }
            logger.info("Writers build complete!");
        } catch (Exception e) {
            logger.error("Error while building writers!", e);
            throw new RuntimeException(e);
        }
    }

    public void put(ClickHouseSinkRequest sinkRequest) {
        try {
            queueCounter.incrementAndGet();
            commonQueue.put(sinkRequest);
        } catch (InterruptedException e) {
            logger.error("InterruptedException while putting sinkRequest into queue", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void waitUntilAllFuturesDone() {
        logger.info("Wait until all futures are done or completed exceptionally, Futures size: {}", futures.size());
        try {
            while (queueCounter.get() > 0 || !futures.isEmpty()) {
                CompletableFuture<Void> future = FutureUtil.allOf(futures);
                try {
                    future.get();
                    futures.removeIf(f -> f.isDone() && !f.isCompletedExceptionally());

                    logger.debug("Futures size after remove operation: {}", futures.size());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            stopWriters();
            futures.clear();
        }
        logger.info("All futures are done or completed exceptionally!");
    }

    private void stopWriters() {
        logger.info("Stopping all writers..");
        if (writeTasks != null && writeTasks.size() > 0) {
            writeTasks.forEach(WriterTask::setStopWorking);
        }
        logger.info("All writers stopped!");
    }

    @Override
    public void close() throws Exception {
        logger.info("All writers is shutting down...");
        try {
            waitUntilAllFuturesDone();
        } finally {
            ThreadUtil.shutdownExecutorService(submitService);
        }
        logger.info("All writers shut down completely!");
    }

    static class WriterTask implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(WriterTask.class);
        private final BlockingQueue<ClickHouseSinkRequest> commonQueue;
        private final AtomicLong queueCounter;
        private final ClickHouseSettings sinkSettings;
        private final List<DataSource> dataSources;
        private final List<CompletableFuture<Boolean>> futures;
        private final int id;
        private volatile boolean isWorking;
        private Integer index = 0;

        WriterTask(int id,
                   List<DataSource> dataSources,
                   BlockingQueue<ClickHouseSinkRequest> commonQueue,
                   ClickHouseSettings settings,
                   List<CompletableFuture<Boolean>> futures,
                   AtomicLong queueCounter) {
            this.id = id;
            this.sinkSettings = settings;
            this.commonQueue = commonQueue;
            this.dataSources = dataSources;
            this.futures = futures;
            this.queueCounter = queueCounter;
        }

        @Override
        public void run() {
            try {
                isWorking = true;

                logger.info("Start writer(id = {}) task", id);
                while (isWorking || commonQueue.size() > 0) {
                    ClickHouseSinkRequest request = commonQueue.poll(3, SECONDS);
                    if (request != null) {
                        CompletableFuture<Boolean> future = new CompletableFuture<>();
                        futures.add(future);
                        flushToClickHouse(request, future);
                    }
                }
                logger.info("Writer(id = {}) task is finished!", id);
            } catch (Exception e) {
                logger.error(String.format("Error while exec writer(id = {}) task!", id), e);
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        private void flushToClickHouse(ClickHouseSinkRequest sinkRequest, CompletableFuture<Boolean> future) {
            String sqlTemplate = TableUtil.genSqlTemplate(sinkRequest.getClazz());
            DataSource dataSource = selectDataSourceByRoundRobin();
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement prepareStatement = conn.prepareStatement(sqlTemplate)) {
                conn.setAutoCommit(false);

                List logs = sinkRequest.getDatas();
                prepareParameters(prepareStatement, sinkRequest.getClazz(), logs);
                long s = System.currentTimeMillis();
                prepareStatement.executeBatch();
                conn.commit();
                long e = System.currentTimeMillis();

                logger.info("Successful flush data to ClickHouse, elapsed = {}ms, batch size = {}, current attempt num = {}", (e - s), sinkRequest.getDatas().size(), sinkRequest.getAttemptCounter());
                future.complete(true);

            } catch (Exception e) {
                logger.error("Error while flush data to ClickHouse!", e);
                handleUnsuccessfulResponse(sinkRequest, future);
            } finally {
                queueCounter.decrementAndGet();
            }
        }

        public DataSource selectDataSourceByRoundRobin() {
            // Rebuild one to avoid the concurrency problem caused by the server's online and offline frequently
            List<DataSource> dss = new ArrayList<>(this.dataSources);

            if (index >= dss.size()) {
                index = 0;
            }

            return dss.get(index++);
        }

        public DataSource selectDataSourceByRandom() {
            // Rebuild one to avoid the concurrency problem caused by the server's online and offline frequently
            List<DataSource> dss = new ArrayList<>(dataSources);

            java.util.Random random = new java.util.Random();
            return dss.get(random.nextInt(dss.size()));
        }

        void prepareParameters(PreparedStatement prepareStatement, Class clazz, List params) throws IllegalAccessException, SQLException {
            for (int i = 0; i < params.size(); i++) {
                Object object = params.get(i);
                Field[] fields = clazz.getDeclaredFields();
                for (int j = 0; j < fields.length; j++) {
                    Field field = fields[j];
                    field.setAccessible(true);

                    Class type = field.getType();
                    Object value = field.get(object);
                    if (value != null && type == Date.class) {
                        value = DateTimeUtil.formatDate((Date) value);
                    } else if (value != null && type == LocalDateTime.class) {
                        value = DateTimeUtil.formatLocalDateTime((LocalDateTime) value);
                    }
                    prepareStatement.setObject(j+1, value);
                }
                prepareStatement.addBatch();
            }
        }

        private void handleUnsuccessfulResponse(ClickHouseSinkRequest sinkRequest, CompletableFuture<Boolean> future) {
            int currentCounter = sinkRequest.getAttemptCounter();
            try {
                if (currentCounter >= sinkSettings.getMaxRetries()) {
                    String msg = "Failed to flush data to ClickHouse, cause: limit of attempts is exceeded!";
                    logger.warn(msg);
                    logFailedRecords(sinkRequest);
                    future.completeExceptionally(new RuntimeException(msg));
                } else {
                    sinkRequest.incrementCounter();
                    logger.warn("Next attempt to flush data to ClickHouse, batch size = {}, current attempt num = {}, max attempt num = {}",
                            sinkRequest.getDatas().size(),
                            sinkRequest.getAttemptCounter(),
                            sinkSettings.getMaxRetries());

                    queueCounter.incrementAndGet();
                    commonQueue.put(sinkRequest);
                    future.complete(false);
                }
            } catch (Exception e) {
                String msg = "Exception while exec handleUnsuccessfulResponse!";
                logger.warn(msg);
                future.completeExceptionally(new RuntimeException(msg));
            }
        }

        private void logFailedRecords(ClickHouseSinkRequest sinkRequest) throws Exception {
            // TODO
        }

        void setStopWorking() {
            isWorking = false;
        }
    }
}
