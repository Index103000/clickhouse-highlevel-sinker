package com.xlvchao.clickhouse.model;

import java.util.Properties;

public class ClickHouseSettings {

    private final int queueMaxCapacity;
    private final int maxRetries;
    private final int writeTimeout;

    private final boolean ignoreSinkExceptionEnabled;


    public ClickHouseSettings(Properties properties) {
        this.ignoreSinkExceptionEnabled = Boolean.parseBoolean(properties.getProperty(ClickHouseConstant.IGNORE_SINK_EXCEPTION_ENABLED, "false"));
        this.queueMaxCapacity = Integer.parseInt(properties.getProperty(ClickHouseConstant.QUEUE_MAX_CAPACITY, "1000"));
        this.maxRetries = Integer.parseInt(properties.getProperty(ClickHouseConstant.RETRY_TIMES, "3"));
        this.writeTimeout = Integer.parseInt(properties.getProperty(ClickHouseConstant.WRITE_TIMEOUT, "3"));
    }

    public boolean isIgnoreSinkExceptionEnabled() {
        return ignoreSinkExceptionEnabled;
    }

    public int getQueueMaxCapacity() {
        return queueMaxCapacity;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    @Override
    public String toString() {
        return "ClickHouseSinkCommonParams{" +
                ", ignoreSinkExceptionEnabled=" + ignoreSinkExceptionEnabled +
                ", queueMaxCapacity=" + queueMaxCapacity +
                ", writeTimeout=" + writeTimeout +
                ", maxRetries=" + maxRetries +
                '}';
    }
}
