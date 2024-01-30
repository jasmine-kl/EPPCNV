package com.hadoop_rd.test;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GCMapper extends Mapper<ChrBinBean, RCGCBean, VIntWritable,GCBean> {
    @Override
    protected void map(ChrBinBean key, RCGCBean value, Context context) throws IOException, InterruptedException {
        context.write(new VIntWritable(value.getGc()),new GCBean(key.getChr(),key.getBin(),value.getRc()));
    }
}