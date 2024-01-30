package com.hadoop_rd.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GCBean implements WritableComparable<GCBean> {
    private Integer chr;
    private Integer bin;
    private Integer rd;

    public GCBean() {
    }

    public Integer getChr() {
        return chr;
    }

    public void setChr(Integer chr) {
        this.chr = chr;
    }

    public Integer getBin() {
        return bin;
    }

    public void setBin(Integer bin) {
        this.bin = bin;
    }

    public Integer getRd() {
        return rd;
    }

    public void setRd(Integer rd) {
        this.rd = rd;
    }

    public GCBean(Integer chr, Integer bin, Integer rd) {
        this.chr = chr;
        this.bin = bin;
        this.rd = rd;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GCBean)) {
            return false;
        }else{
            GCBean r = (GCBean) obj;
            if (this.chr.equals(r.getChr()) && this.bin.equals(r.getBin()) && this.rd.equals(r.getRd())){
                return true;
            }else{
                return false;
            }
        }
    }

    @Override
    public int compareTo(GCBean o) {
        if(this.chr.equals(o.getChr())){

            if(this.bin.equals(o.getBin())){
                return (int)(this.rd - o.getRd())*1000;
            }
            else {
                return this.bin - o.getBin();
            }

        }
        return this.chr - o.getChr();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.chr);
        dataOutput.writeInt(this.bin);
        dataOutput.writeInt(this.rd);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.chr = dataInput.readInt();
        this.bin = dataInput.readInt();
        this.rd = dataInput.readInt();
    }
}

