package com.xlvchao.clickhouse.component;


public class ClickHouseSink implements Sink {
    private final ClickHouseSinkBuffer clickHouseSinkBuffer;

    public ClickHouseSink(ClickHouseSinkBuffer buffer) {
        this.clickHouseSinkBuffer = buffer;
    }

    @Override
    public void put(Object object) throws Exception {
        clickHouseSinkBuffer.put(object);
    }

    @Override
    public void close() throws Exception {
        if (clickHouseSinkBuffer != null) {
            clickHouseSinkBuffer.close();
        }
    }
}
