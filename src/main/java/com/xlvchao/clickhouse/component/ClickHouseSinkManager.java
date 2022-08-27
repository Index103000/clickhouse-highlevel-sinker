package com.xlvchao.clickhouse.component;

import com.google.common.base.Preconditions;
import com.xlvchao.clickhouse.model.ClickHouseSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ClickHouseSinkManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSinkManager.class);
    private final ClickHouseSettings clickHouseSettings;
    private final Properties properties;
    private final ScheduledCheckerAndCleaner scheduledCheckerAndCleaner;
    private final List<CompletableFuture<Boolean>> futures = Collections.synchronizedList(new LinkedList<>());
    private volatile boolean isClosed = false;


    public ClickHouseSinkManager(Properties properties) {
        this.properties = properties;
        this.clickHouseSettings = new ClickHouseSettings(properties);
        this.scheduledCheckerAndCleaner = new ScheduledCheckerAndCleaner(clickHouseSettings, futures);
        logger.info("Build sink manager success, properties = {}", properties);
    }

    public Sink buildSink(Class clazz, int threadNum, int batchSize) {
        Preconditions.checkNotNull(scheduledCheckerAndCleaner);

        ClickHouseWriter clickHouseWriter = new ClickHouseWriter(threadNum, properties, futures);

        ClickHouseSinkBuffer clickHouseSinkBuffer = ClickHouseSinkBuffer.Builder
                .newClickHouseSinkBuffer()
                .withClass(clazz)
                .withBatchSize(batchSize)
                .withWriter(clickHouseWriter)
                .withWriteTimeout(clickHouseSettings.getWriteTimeout())
                .withFutures(futures)
                .build();

        scheduledCheckerAndCleaner.addSinkBuffer(clickHouseSinkBuffer);

        return new ClickHouseSink(clickHouseSinkBuffer);
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws Exception {
        logger.info("Sink manager is shutting down...");
        scheduledCheckerAndCleaner.close();
        isClosed = true;
        logger.info("Sink manager shutdown complete!");
    }

}
