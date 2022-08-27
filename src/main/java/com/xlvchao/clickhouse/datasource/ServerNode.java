package com.xlvchao.clickhouse.datasource;

import java.util.Objects;

public class ServerNode {
    private String ip;
    private Integer port;

    private ServerNode(String ip, Integer port) {
        this.ip = ip;
        this.port = port;
    }

    @Override
    public String toString() {
        return "ServerNode{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerNode that = (ServerNode) o;
        return ip.equals(that.ip) && port.equals(that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }

    public String getIp() {
        return ip;
    }

    public Integer getPort() {
        return port;
    }


    public static final class Builder {
        private String ip;
        private Integer port;

        private Builder() {
        }

        public static Builder newServerNode() {
            return new Builder();
        }

        public Builder withIp(String ip) {
            this.ip = ip;
            return this;
        }

        public Builder withPort(Integer port) {
            this.port = port;
            return this;
        }

        public ServerNode build() {
            return new ServerNode(
                    this.ip,
                    this.port
            );
        }
    }


}
