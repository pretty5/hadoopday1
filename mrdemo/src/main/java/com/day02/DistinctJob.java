package com.day02;

import com.day01.WCMapper;
import com.day01.WCReducer;
import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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


/*
* 分布式统计单词出现的次数
* */
public class DistinctJob {
    public static class DistinctMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            //遍历单词 输出  word 1
            for (int i = 0; i < words.length; i++) {
                Text keyOut = new Text(words[i]);
                //输出
                context.write(keyOut,NullWritable.get());
            }
        }

    }

    public static class DistinctReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
        //reduce每次处理一个key  会被循环调用


        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "distinct");
        //设置job的运行类
        job.setJarByClass(DistinctJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);

        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/wc/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job, new Path("/wcout6"));
        //运行任务
        boolean flag = job.waitForCompletion(true);

    }


}
