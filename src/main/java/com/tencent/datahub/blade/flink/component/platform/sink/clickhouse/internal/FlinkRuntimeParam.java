package com.tencent.datahub.blade.flink.component.platform.sink.clickhouse.internal;

public class FlinkRuntimeParam {

    private int parallelism;
    private int indexOfSubtask;

    public FlinkRuntimeParam() {
    }

    public FlinkRuntimeParam(int parallelism, int indexOfSubtask) {
        this.parallelism = parallelism;
        this.indexOfSubtask = indexOfSubtask;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getIndexOfSubtask() {
        return indexOfSubtask;
    }

    public void setIndexOfSubtask(int indexOfSubtask) {
        this.indexOfSubtask = indexOfSubtask;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FlinkRuntimeParam{");
        sb.append("parallelism=").append(parallelism);
        sb.append(", indexOfSubtask=").append(indexOfSubtask);
        sb.append('}');
        return sb.toString();
    }
}
