package com.xlvchao.clickhouse.model;

import java.util.List;

public class ClickHouseSinkRequest {
    private final List<Object> datas;
    private final Class clazz;
    private int attemptCounter;

    private Exception exception;

    public ClickHouseSinkRequest(List<Object> datas, Class clazz, Exception exception) {
        this.datas = datas;
        this.clazz = clazz;
        this.attemptCounter = 0;
        this.exception = exception;
    }

    public List<Object> getDatas() {
        return datas;
    }

    public void incrementCounter() {
        this.attemptCounter++;
    }

    public int getAttemptCounter() {
        return attemptCounter;
    }

    public Class getClazz() {
        return clazz;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public static final class Builder {
        private List<Object> objects;
        private Class clazz;
        private Exception exception;

        private Builder() {
        }

        public static Builder newClickHouseSinkRequest() {
            return new Builder();
        }

        public Builder withValues(List<Object> objects) {
            this.objects = objects;
            return this;
        }

        public Builder withClass(Class clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder withException(Exception exception) {
            this.exception = exception;
            return this;
        }

        public ClickHouseSinkRequest build() {
            return new ClickHouseSinkRequest(objects, clazz, exception);
        }
    }

    @Override
    public String toString() {
        return "ClickHouseRequestBlank{" +
                "values=" + datas +
                ", attemptCounter=" + attemptCounter +
                ", exception=" + exception +
                '}';
    }
}
