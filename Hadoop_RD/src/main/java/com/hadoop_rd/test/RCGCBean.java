package com.hadoop_rd.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RCGCBean implements WritableComparable<RCGCBean> {

    private int rc;
    private int gc;

    public int getRc() {
        return rc;
    }

    public void setRc(int rc) {
        this.rc = rc;
    }

    public int getGc() {
        return gc;
    }

    public void setGc(int gc) {
        this.gc = gc;
    }

    public RCGCBean() {
    }

    public RCGCBean(int rc, int gc) {
        this.rc = rc;
        this.gc = gc;
    }

    @Override
    public int compareTo(RCGCBean o) {
        if(this.rc == o.getRc()){
            return (int)(this.rc - o.getRc());
        }
        return this.gc - o.getGc();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(rc);
        dataOutput.writeInt(gc);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.rc = dataInput.readInt();
        this.gc = dataInput.readInt();
    }
}
