package com.hadoop_rd.test;




import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.seqdoop.hadoop_bam.ReferenceFragment;

import java.io.IOException;

public class FAMapper extends Mapper<Text, ReferenceFragment, ChrBinBean, VIntWritable> {
    private int chr_num = 21;
    private int BinSize = 1000;
    @Override
    protected void map(Text key, ReferenceFragment value, Context context) throws IOException, InterruptedException {

        String ref = value.getSequence().toString();
        int start = value.getPosition();
        int end = start + ref.length() - 1;
        int GC = 0 ;
        for(int i = 0 ;i < ref.length() ; i++ ){
            char ch = ref.charAt(i);
            if(ch == 'n' || ch == 'N'){
                context.write(new ChrBinBean(new Integer(chr_num),new Integer((start + i)/BinSize)), new VIntWritable(-100*BinSize));
                if(end/BinSize == (start + i)/BinSize){
                    break;
                }else {
                    i = end - start - end%BinSize ;
                    GC  = 0;
                    continue;
                }
            }
            if(( i + start )%BinSize == 0){
                context.write(new ChrBinBean(new Integer(chr_num),new Integer((start + i)/BinSize - 1)) , new VIntWritable(-1*GC));
                GC = 0;
            }
            if(ch == 'g' || ch == 'G' || ch == 'c' || ch == 'C'){
                GC ++ ;
            }


        }
        context.write(new ChrBinBean( new Integer(chr_num) , new Integer(end/BinSize )), new VIntWritable(-1 * GC ) );

    }

}
