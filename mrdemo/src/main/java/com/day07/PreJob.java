package com.day07;

/*
*@ClassName:PreJob
 @Description:TODO
 @Author:
 @Date:2018/10/17 9:44 
 @Version:v1.0
*/

import com.day05.ScoreJob;
import com.day05.ScoreWritable;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "pre");
        //设置job的运行类
        job.setJarByClass(PreJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(PreMapper.class);
        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        /*job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);*/

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/accesslog/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        if (fileSystem.exists(new Path("/out/"))) {
            fileSystem.delete(new Path("/out/"), true);
        }
        ;

        FileOutputFormat.setOutputPath(job, new Path("/out/"));
        //运行任务
        boolean flag = job.waitForCompletion(true);

        /*if(flag){
            FSDataInputStream open = fileSystem.open(new Path("/out/part-r-00000"));
            byte[] buffer = new byte[1024];
            IOUtils.readFully(open,buffer,0,open.available());
            System.out.println(new String(buffer));
        }*/


    }

    //利用正则表达式对数据进行预处理
    public static class PreMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String reg = "(\\d+\\.\\d+\\.\\d+\\.\\d+).*(\\d+\\/\\S+).*(\\\".*\\\")\\s(\\S+).*";
            Pattern pattern = Pattern.compile(reg);
            // 忽略大小写的写法
            // Pattern pat = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(line);
            // 字符串是否与正则表达式相匹配
            boolean rs = matcher.matches();

            if (rs && matcher.groupCount() == 4) {
                String ip = matcher.group(1);
                String date = matcher.group(2);
                String path = matcher.group(3);

                String status = matcher.group(4);
                //对path拆分
                String[] strings = path.split(" ");
                if (strings.length >= 2) {
                    String method = strings[0].substring(1);
                    String url = strings[1];
                    context.write(new Text(ip + "," + date + "," + status + "," + method + "," + url), NullWritable.get());
                }
            }
            //System.out.println(rs);

        }
    }
}
