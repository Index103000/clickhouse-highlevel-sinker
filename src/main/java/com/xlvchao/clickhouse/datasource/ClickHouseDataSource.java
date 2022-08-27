package com.xlvchao.clickhouse.datasource;

import com.google.common.base.Preconditions;
import com.xlvchao.clickhouse.util.ThreadUtil;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDriver;
import ru.yandex.clickhouse.ClickhouseJdbcUrlParser;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 为了支持写本地表，因此自定义数据源（参考自官方ClickHouseDataSource.class）
 *
 * 获取Connection时，支持使用不通的负载均衡策略
 *
 * Created by lvchao on 2022/8/10 18:35
 */
public class ClickHouseDataSource implements DataSource {
    protected final ClickHouseDriver driver;
    protected final String url;
    protected PrintWriter printWriter;
    protected int loginTimeoutSeconds;
    private ClickHouseProperties properties;
    private final String sqlTemplate = "select host_address, port from system.clusters";
    private volatile Set<ServerNode> serverNodes = new HashSet<>();
    private static AtomicInteger pos = new AtomicInteger(0);
    private static final String PREFIX = "jdbc:clickhouse://";
    private static final String SUFFIX = "?socket_timeout=3600000&max_execution_time=3600";

    public ClickHouseDataSource(String url) {
        this(url, new ClickHouseProperties());
    }

    public ClickHouseDataSource(String proxyUrl, Properties info) {
        this(proxyUrl, new ClickHouseProperties(info));

        //Add proxy into the set for initialization use
        serverNodes.add(parseServer(proxyUrl));

        //Achedule update the cluster node infos
        ThreadFactory factory = ThreadUtil.threadFactory("clickhouse-datasource-scheduled-updater");
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(factory);
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try (Connection conn = this.getConnection();
                 PreparedStatement prepareStatement = conn.prepareStatement(sqlTemplate);
                 ResultSet rs = prepareStatement.executeQuery()) {
                serverNodes.addAll(convertToList(rs));
                //Delete the proxy node
                serverNodes.remove(parseServer(proxyUrl));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.MINUTES);
    }

    private ServerNode parseServer(String url) {
        Preconditions.checkNotNull(url);
        int start = url.indexOf("/") + 2;
        int end = url.lastIndexOf("?") == -1 ? url.length() : url.lastIndexOf("?");
        String str = url.substring(start, end);
        String [] arr = str.split(":");

        return ServerNode.Builder
                .newServerNode()
                .withIp(arr[0])
                .withPort(Integer.parseInt(arr[1]))
                .build();
    }

    private List<ServerNode> convertToList(ResultSet rs) throws SQLException {
        List<ServerNode> list = new ArrayList();
        while (rs.next()) {
            ServerNode sn = ServerNode.Builder
                            .newServerNode()
                            .withIp(rs.getString("host_address"))
                            .withPort(8123)
                            .build();

            list.add(sn);
        }
        return list;
    }

    public ClickHouseDataSource(String url, ClickHouseProperties properties) {
        this.driver = new ClickHouseDriver();
        this.loginTimeoutSeconds = 0;
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must be not null");
        } else {
            this.url = url;

            try {
                this.properties = ClickhouseJdbcUrlParser.parse(url, properties.asProperties());
            } catch (URISyntaxException var4) {
                throw new IllegalArgumentException(var4);
            }
        }
    }

    public String getServerByRoundRobin() {
        // Rebuild one to avoid the concurrency problem caused by the server's online and offline frequently
        List<ServerNode> servers = new ArrayList<>(serverNodes);

        if(pos.get() >= servers.size()) {
            pos.set(0);
        }
        ServerNode server = servers.get(pos.getAndIncrement());

        return PREFIX.concat(server.getIp() + ":" + server.getPort()).concat(SUFFIX);
    }

    public String getServerByRandom() {
        // Rebuild one to avoid the concurrency problem caused by the server's online and offline frequently
        List<ServerNode> servers = new ArrayList<>(serverNodes);

        java.util.Random random = new java.util.Random();
        ServerNode server = servers.get(random.nextInt(servers.size()));

        return PREFIX.concat(server.getIp() + ":" + server.getPort()).concat(SUFFIX);
    }

    public ClickHouseConnection getConnection() throws SQLException {
//        return this.driver.connect(this.url, this.properties);
        return this.driver.connect(getServerByRandom(), this.properties);
    }

    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        return this.driver.connect(this.url, this.properties.withCredentials(username, password));
    }

    public String getHost() {
        return this.properties.getHost();
    }

    public int getPort() {
        return this.properties.getPort();
    }

    public String getDatabase() {
        return this.properties.getDatabase();
    }

    public String getUrl() {
        return this.url;
    }

    public ClickHouseProperties getProperties() {
        return this.properties;
    }

    public PrintWriter getLogWriter() throws SQLException {
        return this.printWriter;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        this.printWriter = out;
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeoutSeconds = seconds;
    }

    public int getLoginTimeout() throws SQLException {
        return this.loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(this.getClass())) {
            return iface.cast(this);
        } else {
            throw new SQLException("Cannot unwrap to " + iface.getName());
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    public ClickHouseDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit) {
        this.driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }
}
