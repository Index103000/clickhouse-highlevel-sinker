package com.xlvchao.clickhouse.component;

public interface Sink extends AutoCloseable {
    void put(Object object) throws Exception;
}
