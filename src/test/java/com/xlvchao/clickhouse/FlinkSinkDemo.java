//package com.hihonor.aiops.clickhouse;
//
//import com.hihonor.aiops.clickhouse.component.ClickHouseSinkManager;
//import com.hihonor.aiops.clickhouse.component.Sink;
//import com.hihonor.aiops.clickhouse.pojo.InterfaceLog;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//import java.util.Properties;
//
//
//public class FlinkSinkDemo extends RichSinkFunction<InterfaceLog> {
//    private volatile static transient ClickHouseSinkManager sinkManager;
//    private transient Sink sink;
//
//    public FlinkSinkDemo() {
//    }
//
//    @Override
//    public void open(Configuration config) {
//        // DCL单例模式
//        if (sinkManager == null) {
//            synchronized (com.hihonor.aiops.clickhouse.FlinkSinkDemo.class) {
//                if (sinkManager == null) {
//                    Properties properties = new Properties();
//                    properties.put("clickhouse.hikari.username", "root");
//                    properties.put("clickhouse.hikari.password", "password");
//                    properties.put("clickhouse.hikari.address", "10.68.178.71:8123");
//                    properties.put("clickhouse.hikari.minimumIdle", "5");
//                    properties.put("clickhouse.hikari.maximumPoolSize", "10");
//
//                    properties.put("clickhouse.sink.queueMaxCapacity", "1000");
//                    properties.put("clickhouse.sink.writeTimeout", "1");
//                    properties.put("clickhouse.sink.retryTimes", "3");
//                    properties.put("clickhouse.sink.ignoreSinkExceptionEnabled", "true");
//                    sinkManager = new ClickHouseSinkManager(properties);
//                }
//            }
//        }
//        sink = sinkManager.buildSink(InterfaceLog.class,  3,1000);
//    }
//
//    @Override
//    public void invoke(InterfaceLog log, Context context) {
//        try {
//            sink.put(log);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        if (sink != null) {
//            sink.close();
//        }
//        if (sinkManager != null && !sinkManager.isClosed()) {
//            synchronized (com.hihonor.aiops.clickhouse.FlinkSinkDemo.class) {
//                if (sinkManager != null && !sinkManager.isClosed()) {
//                    sinkManager.close();
//                    sinkManager = null;
//                }
//            }
//        }
//        super.close();
//    }
//}
