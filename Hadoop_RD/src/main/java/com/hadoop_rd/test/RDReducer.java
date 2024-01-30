package com.hadoop_rd.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.IOException;

public class RDReducer extends Reducer<ChrBinBean, VIntWritable,ChrBinBean,RCGCBean> {
    private int chr_num = 21;
    private int BinSize = 1000;
    @Override
    protected void reduce(ChrBinBean key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        int RC = 0;
        int GC = 0;
        for(VIntWritable value : values){
            int v = value.get();
            if(v == -100*BinSize){
                return ;
            }else if(v > 0){
                RC += v;
            }else {
                GC += -1* v;
            }
        }
        key.setBin(key.getBin()+1);


        //GC[i] = int(round(gc_count / binSize, 3) * 1000)
        //现只对chr21做测试

        // RD = 4.204 , GC = 263 => write 23604.204
        context.getCounter(JobMain.MyCounter.RD).increment(RC);
        context.write(key,new RCGCBean(RC,GC));
        //context.write(k,new Text("" + (1.0*RC)/1000 + "\t" + GC));

    }
}