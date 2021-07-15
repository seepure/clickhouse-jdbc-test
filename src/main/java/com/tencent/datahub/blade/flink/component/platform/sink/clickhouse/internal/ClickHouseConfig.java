package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal;

import java.io.Serializable;

public class ClickHouseConfig implements Serializable {

    private String hostAddr;
    private String clusterName;
    private String dbName;
    private String tableName;
    private String username;
    private String pwd;
    private Integer httpPort = 8123;
    private Integer tcpPort;
    private String clusterType;    //写入模式
    private Boolean isDistribute = false;   //是否分布式写入
    private Integer batchSize;
    private Integer batchInterval;

    public String getHostAddr() {
        return hostAddr;
    }

    public void setHostAddr(String hostAddr) {
        this.hostAddr = hostAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(Integer httpPort) {
        this.httpPort = httpPort;
    }

    public Integer getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(Integer tcpPort) {
        this.tcpPort = tcpPort;
    }

    public String getClusterType() {
        return clusterType;
    }

    public void setClusterType(String clusterType) {
        this.clusterType = clusterType;
    }

    public Boolean isDistribute() {
        return isDistribute;
    }

    public void setDistribute(Boolean distribute) {
        isDistribute = distribute;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getBatchInterval() {
        return batchInterval;
    }

    public void setBatchInterval(Integer batchInterval) {
        this.batchInterval = batchInterval;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClickHouseConfig{");
        sb.append("hostAddr='").append(hostAddr).append('\'');
        sb.append(", clusterName='").append(clusterName).append('\'');
        sb.append(", dbName='").append(dbName).append('\'');
        sb.append(", tableName='").append(tableName).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", pwd='").append(pwd).append('\'');
        sb.append(", httpPort=").append(httpPort);
        sb.append(", tcpPort=").append(tcpPort);
        sb.append(", clusterType='").append(clusterType).append('\'');
        sb.append(", isDistribute=").append(isDistribute);
        sb.append(", batchSize=").append(batchSize);
        sb.append(", batchInterval=").append(batchInterval);
        sb.append('}');
        return sb.toString();
    }
}
