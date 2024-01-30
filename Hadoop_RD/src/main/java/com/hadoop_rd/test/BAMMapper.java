package com.hadoop_rd.test;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.IOException;

public class BAMMapper extends Mapper<LongWritable, SAMRecordWritable, ChrBinBean, VIntWritable> {
    private int BinSize = 1000;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        BinSize = Integer.parseInt(context.getConfiguration().get("BinSize"));
    }

    @Override
    protected void map(LongWritable key, SAMRecordWritable value, Context context) throws IOException, InterruptedException {

        SAMRecord samRecord = value.get();
        int chrnum = 0;
        if (samRecord.getReferenceName().length() <= 3) {
            return;
        }
        String chr = samRecord.getReferenceName().substring(3);

        if (StringUtils.isNumeric(chr)) {
            chrnum = Integer.parseInt(chr);
        } else if (chr.equals("M")) {
            chrnum = 23;
        } else if (chr.equals("X")) {
            chrnum = 24;
        } else if (chr.equals("Y")) {
            chrnum = 25;
        }

        int start = samRecord.getAlignmentStart() - 1;
        int end = samRecord.getAlignmentEnd() - 1;
        if (start / BinSize == end / BinSize) {
            context.write(new ChrBinBean(chrnum, start / BinSize), new VIntWritable(end - start + 1));
        } else {
            context.write(new ChrBinBean(chrnum, start / BinSize), new VIntWritable((end / BinSize) * BinSize - start));

            context.write(new ChrBinBean(chrnum, end / BinSize), new VIntWritable(end % BinSize + 1));
        }


    }

}