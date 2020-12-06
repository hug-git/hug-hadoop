package com.atguigu.sumflow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private Long inFlow;
    private Long outFlow;
    private Long sumFlow;

    public void setFlow(Long inFlow, Long outFlow) {
        this.inFlow = inFlow;
        this.outFlow = outFlow;
        this.sumFlow = inFlow + outFlow;
    }

    public Long getInFlow() {
        return inFlow;
    }

    public void setInFlow(Long inFlow) {
        this.inFlow = inFlow;
    }

    public Long getOutFlow() {
        return outFlow;
    }

    public void setOutFlow(Long outFlow) {
        this.outFlow = outFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return inFlow + "\t" + outFlow + "\t" + sumFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(inFlow);
        out.writeLong(outFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.inFlow = in.readLong();
        this.outFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public int compareTo(FlowBean o) {
        return Long.compare(this.sumFlow, o.sumFlow);
    }
}
