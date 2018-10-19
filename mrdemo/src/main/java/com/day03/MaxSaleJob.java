package com.day03;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/*
*@ClassName:DistinctJob
 @Description:TODO
 @Author:
 @Date:2018/10/10 14:04 
 @Version:v1.0
*/
public class MaxSaleJob {
    public static class MaxSaleMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        //int count=0;
        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                //context.write(NullWritable.get(),value);
            context.write(value,NullWritable.get());

        }

    }

    public static class MaxSaleReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        //reduce每次处理一个key  会被循环调用
        int max=-1;
        //ArrayList<Text>  citys=new ArrayList<Text>();
        Text maxRecord=null;
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {


            String[] strings = key.toString().split(",");
            String sale = strings[2];
            if (Integer.parseInt(sale)>max){
                max=Integer.parseInt(sale);

                maxRecord=new Text(strings[1]);
            }else if (Integer.parseInt(sale)==max){
                maxRecord=new Text(maxRecord.toString()+","+strings[1]);
            }

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //super.setup(context);


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            context.write(maxRecord,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site-rpc.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "maxsale");
        //设置job的运行类
        //job.setJarByClass(MaxSaleJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(MaxSaleMapper.class);
        job.setReducerClass(MaxSaleReducer.class);

        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/home/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job, new Path("/out/"));
        //运行任务
        boolean flag = job.waitForCompletion(true);

    }


}
