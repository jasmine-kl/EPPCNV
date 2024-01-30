package com.hadoop_rd.test;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class GCPartitioner implements Partitioner<VIntWritable,GCBean> {
    @Override
    public int getPartition(VIntWritable key, GCBean value, int i) {

        return value.getChr();
    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
