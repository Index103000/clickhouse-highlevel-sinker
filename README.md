# **clickhouse-highlevel-sinker**



## 1、简介

ClickHouse批量写SDK，支持在 **Springboot 和 Flink** 中使用，人性化、灵活的配置、高性能、极简依赖！



## 2、使用方式

### 2.1 项目pom.xml中引入依赖：

```xml
<dependency>
    <groupId>com.xlvchao.clickhouse</groupId>
    <artifactId>clickhouse-highlevel-sinker</artifactId>
    <version>1.0.3</version>
</dependency>
```



### 2.2 Springboot中使用

**定义POJO：**

```java
import TableName;
import lombok.Data;

@Data
@TableName("aiops_local.interfacelog")
public class InterfaceLog {
    private String product; //业务
    private String service; //服务
    private String itf; //接口
    private String accountErrorCode;
    private String addn;
    private Long aggregateDelay;
    private Long avgAggregateDelay;
    private Long nonAggregatedDelay;
    private Long latency;
    private Double avgLatency;
    private String destinationId;
    private String errorDetail;
    private Long fail;
    private String ip;
    private String ipCity;
    private String ipCountry;
    private String ipProvince;
    private String itfGroup;
    private String returnCode;
    private String sourceId;
    private Long success;
    private Long total;
    private Long totalInterfaceLogAggregateDelay;
    private String type;
    private LocalDateTime sysTime;
    private LocalDateTime time;
}
```

**注册Bean:**

```java
import ClickHouseSinkManager;
import Sink;
import com.hihonor.basecloud.cloudsoa.security.cipher.AESCryptor;
import com.hihonor.basecloud.cloudsoa.security.cipher.CipherTextCodec;
import com.hihonor.basecloud.cloudsoa.security.cipher.CryptoAlg;
import com.hihonor.basecloud.cloudsoa.security.exception.CipherException;
import com.hihonor.basecloud.cloudsoa.security.exception.KeyProviderException;
import com.hihonor.basecloud.cloudsoa.security.storage.KeyStorage;
import com.hihonor.basecloud.cloudsoa.security.workkey.WorkKeyProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.Properties;

@Configuration
@Slf4j
public class ClickHouseConfig {

    @Resource
    private ClickHouseSinkManager clickHouseSinkManager;

    //注意：一个微服务实例中，只能注册一个ClickHouseSinkManager！！！
    @Bean
    public ClickHouseSinkManager clickHouseSinkManager() {
        Properties properties = new Properties();
        properties.put("clickhouse.hikari.username", "username");
        properties.put("clickhouse.hikari.password", "password");
        properties.put("clickhouse.hikari.addresses", "10.68.178.71:8123,10.68.177.248:8123");
        properties.put("clickhouse.hikari.minimumIdle", "5");
        properties.put("clickhouse.hikari.maximumPoolSize", "10");

        properties.put("clickhouse.sink.queueMaxCapacity", "1000"); //公共消费队列容量（即最大能承载多少批次），默认是1000
        properties.put("clickhouse.sink.writeTimeout", "3"); //写超时时间（单位秒），即当Buffer中数据超过该时间还未被flush则立马刷到上述公共消费队列中，默认是3
        properties.put("clickhouse.sink.retryTimes", "3"); //下沉到CK时如果失败的重试次数，默认是3
        properties.put("clickhouse.sink.ignoreSinkExceptionEnabled", "true"); //下沉到CK时是否忽略写异常，默认为true
        return new ClickHouseSinkManager(properties);
    }

    @Bean
    public Sink interfaceLogSink() {
        return clickHouseSinkManager.buildSink(InterfaceLog.class, 3, 1000);
    }
}
```



### 2.3 Flink中使用

**定义POJO：**

```java
import TableName;
import lombok.Data;

@Data
@TableName("aiops_local.interfacelog")
public class InterfaceLog {
    private String product; //业务
    private String service; //服务
    private String itf; //接口
    private String accountErrorCode;
    private String addn;
    private Long aggregateDelay;
    private Long avgAggregateDelay;
    private Long nonAggregatedDelay;
    private Long latency;
    private Double avgLatency;
    private String destinationId;
    private String errorDetail;
    private Long fail;
    private String ip;
    private String ipCity;
    private String ipCountry;
    private String ipProvince;
    private String itfGroup;
    private String returnCode;
    private String sourceId;
    private Long success;
    private Long total;
    private Long totalInterfaceLogAggregateDelay;
    private String type;
    private LocalDateTime sysTime;
    private LocalDateTime time;
}
```

**定义Sink:**

```java
import com.hihonor.aiops.clickhouse.component.ClickHouseSinkManager;
import com.hihonor.aiops.clickhouse.component.Sink;
import com.hihonor.aiops.clickhouse.pojo.InterfaceLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Properties;


public class FlinkSinkDemo extends RichSinkFunction<InterfaceLog> {
    private volatile static transient ClickHouseSinkManager sinkManager;
    private transient Sink sink;

    public FlinkSinkDemo() {
    }

    @Override
    public void open(Configuration config) {
        // DCL单例模式
        if (sinkManager == null) {
            synchronized (FlinkSinkDemo.class) {
                if (sinkManager == null) {
                    Properties properties = new Properties();
                    properties.put("clickhouse.hikari.username", "root");
                    properties.put("clickhouse.hikari.password", "AigWNjWH");
                    properties.put("clickhouse.hikari.addresses", "10.68.178.71:8123,10.68.177.248:8123");
                    properties.put("clickhouse.hikari.minimumIdle", "5");
                    properties.put("clickhouse.hikari.maximumPoolSize", "10");

                    properties.put("clickhouse.sink.queueMaxCapacity", "1000"); //公共消费队列容量（即最大能承载多少批次），默认是1000
                    properties.put("clickhouse.sink.writeTimeout", "3"); //写超时时间（单位秒），即当Buffer中数据超过该时间还未被flush则立马刷到上述公共消费队列中，默认是3
                    properties.put("clickhouse.sink.retryTimes", "3"); //下沉到CK时如果失败的重试次数，默认是3
                    properties.put("clickhouse.sink.ignoreSinkExceptionEnabled", "true"); //下沉到CK时是否忽略写异常，默认为true
                    sinkManager = new ClickHouseSinkManager(properties);
                }
            }
        }
        sink = sinkManager.buildSink(InterfaceLog.class,  3,1000);
    }

    @Override
    public void invoke(InterfaceLog log, Context context) {
        try {
            sink.put(log);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (sink != null) {
            sink.close();
        }
        if (sinkManager != null && !sinkManager.isClosed()) {
            synchronized (FlinkSinkDemo.class) {
                if (sinkManager != null && !sinkManager.isClosed()) {
                    sinkManager.close();
                    sinkManager = null;
                }
            }
        }
        super.close();
    }
}
```



## 3、更新日志

### 1.0.3
- 优化代码
- 初始化数据源时不再通过代理查询IP列表，直接通过配置传入IP列表

### 1.0.2
- 优化代码
- 采用Hikari连接池管理数据库连接

### 1.0.1
- 更加灵活的配置
- 支持Flink

### 1.0.0
- 支持Springboot
- 完成批量写
