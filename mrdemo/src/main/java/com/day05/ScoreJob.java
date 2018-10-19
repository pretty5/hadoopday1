package com.day05;

import com.day03.MaxSaleJob;
import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
*@ClassName:ScoreJob
 @Description:TODO
 @Author:
 @Date:2018/10/15 10:27 
 @Version:v1.0
*/
public class ScoreJob {
    public static class ScoreMapper extends Mapper<LongWritable,Text,ScoreWritable,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            String[] grades = value.toString().split(",");
            ScoreWritable score = new ScoreWritable(Integer.parseInt(grades[0]), Integer.parseInt(grades[1]), Integer.parseInt(grades[2]));
            context.write(score,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {



        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "score");
        //设置job的运行类
        job.setJarByClass(ScoreJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(ScoreMapper.class);
        //map输出类型
        job.setMapOutputKeyClass(ScoreWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        /*job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);*/

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/score/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        //判断，如果存在，则删除
        if(fileSystem.exists(new Path("/out/"))){
            fileSystem.delete(new Path("/out/"),true);
        }

        FileOutputFormat.setOutputPath(job, new Path("/out/"));
        //运行任务
        boolean flag = job.waitForCompletion(true);

        //打印结果
        if(flag){
            FSDataInputStream open = fileSystem.open(new Path("/out/part-r-00000"));
            byte[] buffer = new byte[1024];
            IOUtils.readFully(open,buffer,0,open.available());
            System.out.println(new String(buffer));
        }
    }


}
