package com.hadoop_rd.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChrBinBean implements WritableComparable<ChrBinBean> {
    private Integer chr;
    private Integer bin;

    @Override
    public String toString() {
        return chr.toString() + "\t" + bin.toString();
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

    public ChrBinBean() {
    }

    public ChrBinBean(Integer chr, Integer bin) {
        this.chr = chr;
        this.bin = bin;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ChrBinBean)) {
            return false;
        }else{
            ChrBinBean r = (ChrBinBean) obj;
            if (this.chr.equals(r.getChr()) && this.bin.equals(r.getBin())){
                return true;
            }else{
                return false;
            }
        }
    }

    @Override
    public int hashCode() {
        return this.chr*1000000 + this.bin;
    }



    @Override
    public int compareTo(ChrBinBean o) {
        if(this.chr.equals(o.getChr())){
            return this.bin - o.getBin();
        }
        return this.chr - o.getChr();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(chr);
        dataOutput.writeInt(bin);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.chr = dataInput.readInt();
        this.bin = dataInput.readInt();
    }
}
