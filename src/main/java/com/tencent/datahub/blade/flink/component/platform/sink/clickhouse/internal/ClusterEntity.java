package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal;

public class ClusterEntity {

    private String cluster;

    private int shardNum;

    private int shardWeight;

    private int replicaNum;

    private String hostName;

    private String hostAddress;

    private int port;

    public ClusterEntity() {
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public int getShardNum() {
        return shardNum;
    }

    public void setShardNum(int shardNum) {
        this.shardNum = shardNum;
    }

    public int getShardWeight() {
        return shardWeight;
    }

    public void setShardWeight(int shardWeight) {
        this.shardWeight = shardWeight;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
