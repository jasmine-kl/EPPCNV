package com.hadoop_rd.test;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class GCReducer extends Reducer<VIntWritable, GCBean, Text, NullWritable> {

    private List<Double> global_rd_ave = new ArrayList<Double>(24);
    private int BinSize = 1000;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Double global_rd_ave21 = Double.parseDouble(context.getConfiguration().get("global_rd_ave21"));
        global_rd_ave.set(21,global_rd_ave21);

    }
    @Override
    protected void reduce(VIntWritable key, Iterable<GCBean> values, Context context) throws IOException, InterruptedException {

        List<GCBean> valuesList = new LinkedList<GCBean>();
        double rd_sum = 0.0;
        int record_count = 0;
        int chrnum = 0;
        for(GCBean v :values){
            valuesList.add(new GCBean(v.getChr(),v.getBin(),v.getRd()));
            chrnum = v.getChr();
            rd_sum += 1.0*v.getRd()/BinSize;
            record_count ++;
        }
        if(record_count == 0){
            return;
        }if(record_count == 1){
            context.write(new Text("" + valuesList.get(0).getChr() + "\t" + valuesList.get(0).getBin()
                    + "\t" + valuesList.get(0).getRd()*1.0/BinSize ),NullWritable.get());
            return ;
        }
        double rd_ave = global_rd_ave.get(chrnum);
        double bias = 1.0 * rd_ave / (rd_sum / record_count) ;
        for(GCBean v:valuesList){
            context.write(new Text("" + v.getChr() + "\t" + v.getBin()
                    + "\t" + 1.0*v.getRd()*bias/BinSize  ),NullWritable.get());
        }



    }
}
