package com.xlvchao.clickhouse.pojo;

import com.xlvchao.clickhouse.annotation.TableName;

import java.time.LocalDateTime;

/**
 * 接口监控
 *
 * Created by lvchao on 2022/7/26 19:47
 */
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

    public long getAggregateDelay() {
        return aggregateDelay;
    }

    public void setAggregateDelay(long aggregateDelay) {
        this.aggregateDelay = aggregateDelay;
    }

    public long getAvgAggregateDelay() {
        return avgAggregateDelay;
    }

    public void setAvgAggregateDelay(long avgAggregateDelay) {
        this.avgAggregateDelay = avgAggregateDelay;
    }

    public long getNonAggregatedDelay() {
        return nonAggregatedDelay;
    }

    public void setNonAggregatedDelay(long nonAggregatedDelay) {
        this.nonAggregatedDelay = nonAggregatedDelay;
    }

    public long getLatency() {
        return latency;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }

    public long getFail() {
        return fail;
    }

    public void setFail(long fail) {
        this.fail = fail;
    }

    public long getSuccess() {
        return success;
    }

    public void setSuccess(long success) {
        this.success = success;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getTotalInterfaceLogAggregateDelay() {
        return totalInterfaceLogAggregateDelay;
    }

    public void setTotalInterfaceLogAggregateDelay(long totalInterfaceLogAggregateDelay) {
        this.totalInterfaceLogAggregateDelay = totalInterfaceLogAggregateDelay;
    }

    public LocalDateTime getSysTime() {
        return sysTime;
    }

    public void setSysTime(LocalDateTime sysTime) {
        this.sysTime = sysTime;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public void setTime(LocalDateTime time) {
        this.time = time;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getItf() {
        return itf;
    }

    public void setItf(String itf) {
        this.itf = itf;
    }

    public String getAccountErrorCode() {
        return accountErrorCode;
    }

    public void setAccountErrorCode(String accountErrorCode) {
        this.accountErrorCode = accountErrorCode;
    }

    public String getAddn() {
        return addn;
    }

    public void setAddn(String addn) {
        this.addn = addn;
    }

    public Double getAvgLatency() {
        return avgLatency;
    }

    public void setAvgLatency(Double avgLatency) {
        this.avgLatency = avgLatency;
    }

    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    public String getErrorDetail() {
        return errorDetail;
    }

    public void setErrorDetail(String errorDetail) {
        this.errorDetail = errorDetail;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIpCity() {
        return ipCity;
    }

    public void setIpCity(String ipCity) {
        this.ipCity = ipCity;
    }

    public String getIpCountry() {
        return ipCountry;
    }

    public void setIpCountry(String ipCountry) {
        this.ipCountry = ipCountry;
    }

    public String getIpProvince() {
        return ipProvince;
    }

    public void setIpProvince(String ipProvince) {
        this.ipProvince = ipProvince;
    }

    public String getItfGroup() {
        return itfGroup;
    }

    public void setItfGroup(String itfGroup) {
        this.itfGroup = itfGroup;
    }

    public String getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(String returnCode) {
        this.returnCode = returnCode;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public void setAggregateDelay(Long aggregateDelay) {
        this.aggregateDelay = aggregateDelay;
    }

    public void setAvgAggregateDelay(Long avgAggregateDelay) {
        this.avgAggregateDelay = avgAggregateDelay;
    }

    public void setNonAggregatedDelay(Long nonAggregatedDelay) {
        this.nonAggregatedDelay = nonAggregatedDelay;
    }

    public void setLatency(Long latency) {
        this.latency = latency;
    }

    public void setFail(Long fail) {
        this.fail = fail;
    }

    public void setSuccess(Long success) {
        this.success = success;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public void setTotalInterfaceLogAggregateDelay(Long totalInterfaceLogAggregateDelay) {
        this.totalInterfaceLogAggregateDelay = totalInterfaceLogAggregateDelay;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
