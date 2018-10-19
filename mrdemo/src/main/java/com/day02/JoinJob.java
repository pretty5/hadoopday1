package com.day02;

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
* 需求：class.txt  内容：1，human
*                        2,animal
*        student.txt 内容：1，zhangsan,1
*                           2,lisi,1
*                           3,tom,2
*                           2,jerry,2
*      结果:2,lisi,human
            1,zhangsan,human
            4,tom,animal
            3,jerry,animal
*
* */

public class JoinJob {
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String filename = inputSplit.getPath().getName();
            String line = value.toString();
            String[] strings = line.split(",");
            if (filename.contains("class")) {
                String keyout = strings[0];
                String valueout = strings[1];
                context.write(new Text(keyout), new Text(valueout));
            } else {
                String keyout = strings[2];
                String valueout = strings[0] + "," + strings[1];
                context.write(new Text(keyout), new Text(valueout));
            }

        }

    }


    public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
        //reduce每次处理一个key  会被循环调用

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //value : human    value ：1,zhangsan
            Iterator<Text> iterator = values.iterator();
            ArrayList<String> arrayList = new ArrayList<String>();

            while (iterator.hasNext()) {
                arrayList.add(iterator.next().toString());
            }
            String primary = "";
            for (int i = 0; i < arrayList.size(); i++) {
                String value = arrayList.get(i);
                if (!value.contains(",")) {
                    primary = value;
                }
            }

            for (int i = 0; i < arrayList.size(); i++) {
                String value = arrayList.get(i);
                if (value.contains(",")) {
                    context.write(new Text(value +","+ primary), NullWritable.get());
                }
            }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration coreSiteConf = new Configuration();

            coreSiteConf.addResource(Resources.getResource("core-site.xml"));
            //设置一个任务
            Job job = Job.getInstance(coreSiteConf, "Join");
            //设置job的运行类
            job.setJarByClass(JoinJob.class);
            //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
            //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
            //设置Map和Reduce处理类
            job.setMapperClass(JoinJob.JoinMapper.class);
            job.setReducerClass(JoinJob.JoinReducer.class);

            //map输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置job/reduce输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            //设置任务的输入路径
            FileInputFormat.addInputPath(job, new Path("/join/"));
            // FileInputFormat.addInputPath(job, new Path("/wc/"));

            FileOutputFormat.setOutputPath(job, new Path("/joinout1"));
            //运行任务
            boolean flag = job.waitForCompletion(true);

        }

    }
}