package com.day01;

/*
*@ClassName:WCJob
 @Description:TODO
 @Author:
 @Date:2018/10/9 15:55 
 @Version:v1.0
*/
//设定任务的运行

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
//mapred是hadoop 1.x的api
//mapreduce是hadoop 2.x的api


public class WCJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "wc");
        //设置job的运行类
        job.setJarByClass(WCJob.class);
        //远程连接操作linux提交
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCCombiner.class);
        job.setReducerClass(WCReducer.class);
        //设置reducer的个数
        job.setNumReduceTasks(2);
        job.setPartitionerClass(WCPartitioner.class);
        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置job输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/wc/"));
        FileOutputFormat.setOutputPath(job, new Path("/wcout5"));
        //运行任务
        boolean flag = job.waitForCompletion(true);


    }
}
