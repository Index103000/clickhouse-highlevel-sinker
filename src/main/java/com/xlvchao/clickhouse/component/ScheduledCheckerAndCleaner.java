package com.xlvchao.clickhouse.component;

import com.xlvchao.clickhouse.model.ClickHouseSettings;
import com.xlvchao.clickhouse.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.xlvchao.clickhouse.util.ThreadUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;

public class ScheduledCheckerAndCleaner implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledCheckerAndCleaner.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final List<ClickHouseSinkBuffer> clickHouseSinkBuffers = new ArrayList<>();
    private final List<CompletableFuture<Boolean>> futures;
    private final Predicate<CompletableFuture<Boolean>> filter;


    public ScheduledCheckerAndCleaner(ClickHouseSettings clickHouseSettings, List<CompletableFuture<Boolean>> futures) {
        this.futures = futures;
        this.filter = getFuturesFilter(clickHouseSettings.isIgnoreSinkExceptionEnabled());

        ThreadFactory factory = ThreadUtil.threadFactory("scheduled-checker-and-cleaner");
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(factory);
        this.scheduledExecutorService.scheduleWithFixedDelay(getTask(), clickHouseSettings.getWriteTimeout(), clickHouseSettings.getWriteTimeout(), TimeUnit.SECONDS);
    }

    public void addSinkBuffer(ClickHouseSinkBuffer clickHouseSinkBuffer) {
        synchronized (this) {
            clickHouseSinkBuffers.add(clickHouseSinkBuffer);
        }
        logger.debug("Add clickHouseSinkBuffer success, target table = {}", TableUtil.getTableName(clickHouseSinkBuffer.getClazz()));
    }

    private Runnable getTask() {
        return () -> {
            synchronized (this) {
                logger.debug("I am doing cleanup futures and checkup clickHouseSinkBuffers!");
                futures.removeIf(filter);
                clickHouseSinkBuffers.forEach(ClickHouseSinkBuffer::tryAddToCommonQueue);
            }
        };
    }

    private static Predicate<CompletableFuture<Boolean>> getFuturesFilter(boolean ignoringExceptionEnabled) {
        if (ignoringExceptionEnabled) {
            return CompletableFuture::isDone;
        } else {
            return f -> f.isDone() && !f.isCompletedExceptionally();
        }
    }

    @Override
    public void close() throws Exception {
        logger.info("ScheduledCheckerAndCleaner is shutting down...");
        ThreadUtil.shutdownExecutorService(scheduledExecutorService);
        logger.info("ScheduledCheckerAndCleaner shutdown complete!");
    }
}
