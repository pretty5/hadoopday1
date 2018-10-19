package com.day02;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/*
*@ClassName:DistinctJob
 @Description:TODO
 @Author:
 @Date:2018/10/10 14:04 
 @Version:v1.0
*/
public class InverseIndexJob {
    public static class InverseIndexMapper extends Mapper<LongWritable,Text,Text,Text>{
        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            Path path = inputSplit.getPath();
            String[] strings = value.toString().split(" ");
            for (int i = 0; i < strings.length; i++) {
                context.write(new Text(strings[i]),new Text(path.toString()));
            }

        }

    }

    public static class InverseIndexReducer extends Reducer<Text,Text,Text,Text> {
        //reduce每次处理一个key  会被循环调用


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            StringBuilder fileNames = new StringBuilder();
            while (iterator.hasNext()){
                Text fileName = iterator.next();
                fileNames.append(",").append(fileName.toString());
            }
            context.write(key,new Text(fileNames.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "inverseindex");
        //设置job的运行类
        job.setJarByClass(InverseIndexJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(InverseIndexMapper.class);
        job.setReducerClass(InverseIndexReducer.class);

        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/wc/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job, new Path("/wcout2"));
        //运行任务
        boolean flag = job.waitForCompletion(true);

    }


}
