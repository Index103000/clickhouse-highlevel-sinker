package com.xlvchao.clickhouse.model;

public final class ClickHouseConstant {
    private ClickHouseConstant() {
    }

    public static final String QUEUE_MAX_CAPACITY = "clickhouse.sink.queueMaxCapacity";
    public static final String WRITE_TIMEOUT = "clickhouse.sink.writeTimeout"; //The unit is second
    public static final String RETRY_TIMES = "clickhouse.sink.retryTimes";
    public static final String IGNORE_SINK_EXCEPTION_ENABLED = "clickhouse.sink.ignoreSinkExceptionEnabled";
}
