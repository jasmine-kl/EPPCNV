package com.hadoop_rd.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import java.io.IOException;

public class JobMain {
    private  static String HADOOP_PATH  ;
    private  static String FILE_PATH ;
    private static FileService fileService ;
    private  static String BAM_IN ;
    private  static String FA_IN ;
    private  static String RD_OUT ;
    private static String GC_IN ;
    private  static String GC_OUT;
    private static int BinSize;

    enum MyCounter{
        RD
    }
    public static  void init(){
        //获取用户配置信息
        //初始化文件系统
        HADOOP_PATH  = "hdfs://hadoop:9000";
        FILE_PATH = "/GraProject/Function3/test";
        BAM_IN = "/GraProject/Function3/test/bam";
        FA_IN = "/GraProject/Function3/test/ref/chr21";
        RD_OUT = HADOOP_PATH + FILE_PATH + "/output" + System.currentTimeMillis();
        GC_IN = RD_OUT;
        GC_OUT = "/GraProject/Function3/test/output";

        BinSize = 1000;
        fileService =  new FileService(HADOOP_PATH);
        if(fileService.exist(RD_OUT)){
            fileService.deleteDir(RD_OUT);
        }
        if(fileService.exist(GC_OUT)){
            fileService.deleteDir(GC_OUT);
        }



    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        init();
        Configuration configuration = new Configuration();
        configuration.set("BinSize","" + BinSize);
//        configuration.set("key","value");
//        2.获取一个Job,通过配置文件获取
        Job job = Job.getInstance(configuration);
//        3.指定jar的位置
        job.setJarByClass(com.hadoop_rd.test.JobMain.class);
        //多输入设置 MultipleInputs可以加载不同路径的输入文件，并且每个路径可用不同的mapper
        MultipleInputs.addInputPath(job,new Path(BAM_IN), BAMInputFormat.class, BAMMapper.class);
        MultipleInputs.addInputPath(job,new Path(FA_IN), FastaInputFormat.class, FAMapper.class);
        job.setMapOutputKeyClass(ChrBinBean.class);
        job.setMapOutputValueClass(VIntWritable.class);
//        7.指定Reducer运行类
        job.setReducerClass(RDReducer.class);
//        8.指定Reducer输出的key的类型
        job.setOutputKeyClass(ChrBinBean.class);
//        9.指定Reducer输出value的类型
        job.setOutputValueClass(RCGCBean.class);
//        通过Job设置自定义分区类
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job , new Path(RD_OUT));
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.NONE);
        boolean b = job.waitForCompletion(true);
        double global_rd_ave = 0.0;
        if (!b){
            System.out.println("RDJob failed!");
            return ;
        }else {
            System.out.println("RDJob Success!");
            long rc_sum = job.getCounters().findCounter(MyCounter.RD).getValue();
            long bins =  job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
            global_rd_ave = 1.0*rc_sum/1000/bins;
        }


        configuration = new Configuration();
//        2.获取一个Job,通过配置文件获取
        configuration.set("global_rd_ave21",""+global_rd_ave);
        configuration.set("BinSize",""+ BinSize);

        job = Job.getInstance(configuration);

//        3.指定jar的位置
        job.setJarByClass(com.hadoop_rd.test.JobMain.class);

//        4.指定Mapper运行类
        job.setMapperClass(GCMapper.class);

//        5.指定Mapper输出的key的类型
        job.setMapOutputKeyClass(VIntWritable.class);

//        6.指定Mapper输出的value的类型
        job.setMapOutputValueClass(GCBean.class);

//        7.指定Reducer运行类
        job.setReducerClass(GCReducer.class);

//        8.指定Reducer输出的key的类型
        job.setOutputKeyClass(Text.class);

//        9.指定Reducer输出value的类型
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(RD_OUT));

        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,new Path(GC_OUT));
        //提交job
        b = job.waitForCompletion(true);

        if (!b){
            System.out.println("GCJob failed!");
        }else {
            fileService.deleteDir(RD_OUT);
            System.out.println("GCJob Success!");

        }

    }
}
