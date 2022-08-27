package com.xlvchao.clickhouse;

import com.xlvchao.clickhouse.component.ClickHouseSinkManager;
import com.xlvchao.clickhouse.component.Sink;
import com.xlvchao.clickhouse.pojo.InterfaceLog;
import com.xlvchao.clickhouse.util.CommonUtil;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;

/**
 * Created by lvchao on 2022/8/1 15:44
 */
public class SpringbootDemo {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("clickhouse.hikari.username", "root");
        properties.put("clickhouse.hikari.password", "AigWNjWH");
        properties.put("clickhouse.hikari.addresses", "10.68.178.71:8123,10.68.177.248:8123");
        properties.put("clickhouse.hikari.minimumIdle", "10");
        properties.put("clickhouse.hikari.maximumPoolSize", "150");

        properties.put("clickhouse.sink.queueMaxCapacity", "1000");
        properties.put("clickhouse.sink.writeTimeout", "1");
        properties.put("clickhouse.sink.retryTimes", "3");
        properties.put("clickhouse.sink.ignoreSinkExceptionEnabled", "true");
        ClickHouseSinkManager sinkManager = new ClickHouseSinkManager(properties);
        Sink sink = sinkManager.buildSink(InterfaceLog.class, 10, 1000);


        for (int i = 1; i <= 100000000; i++) {
            InterfaceLog log = new InterfaceLog();

            Map<String, String> logInfo = CommonUtil.genLogInfo();
            log.setProduct(logInfo.get("product"));
            log.setService(logInfo.get("service"));
            log.setItf(logInfo.get("itf"));

            log.setAccountErrorCode("000");
            log.setAddn("no addn");
            log.setAggregateDelay(1);
            log.setAvgAggregateDelay(1);
            log.setNonAggregatedDelay(0);
            log.setLatency(30);
            log.setAvgLatency(25D);
            log.setDestinationId("id-234");
            log.setErrorDetail("no error");
            log.setFail(0);
            log.setIp("127.0.0.1");
            log.setIpCity("深圳");
            log.setIpCountry("中国");
            log.setIpProvince("广东");
            log.setItfGroup("itfGroup");
            log.setReturnCode("000");
            log.setSourceId("id-123");
            log.setSuccess(1);
            log.setTotal(100);
            log.setTotalInterfaceLogAggregateDelay(100);
            log.setType("cli");

            LocalDateTime now = LocalDateTime.now();
            log.setSysTime(now);
            log.setTime(now);
            sink.put(log);
        }

        Thread.sleep(100000000);

    }

}
