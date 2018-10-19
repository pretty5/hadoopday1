package com.day08;

/*
*@ClassName:PreJob
 @Description:TODO
 @Author:
 @Date:2018/10/17 9:44 
 @Version:v1.0
*/

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreParquetJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration coreSiteConf = new Configuration();

        String writeSchema = "message example {\n" +
                "required binary ip;\n" +
                "required binary date;\n" +
                "required int32 status;\n" +
                "required binary method;\n" +
                "required binary url;\n" +
                "}";

        coreSiteConf.set("parquet.example.schema", writeSchema);

        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "pre");
        //设置job的运行类
        job.setJarByClass(PreParquetJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);



        job.setOutputFormatClass(ParquetOutputFormat.class);
        //设置job/reduce输出类型
       // job.setOutputKeyClass(Group.class);
        job.setOutputValueClass(Group.class);

        //GroupWriteSupport.setSchema(MessageTypeParser.parseMessageType(writeSchema), coreSiteConf);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/accesslog/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        if (fileSystem.exists(new Path("/out/"))) {
            fileSystem.delete(new Path("/out/"), true);
        }

        ParquetOutputFormat.setOutputPath(job, new Path("/out/"));
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

        //运行任务
        boolean flag = job.waitForCompletion(true);

        /*if(flag){
            FSDataInputStream open = fileSystem.open(new Path("/out/part-r-00000"));
            byte[] buffer = new byte[1024];
            IOUtils.readFully(open,buffer,0,open.available());
            System.out.println(new String(buffer));
        }*/


    }

    public static class PreReducer extends Reducer<Text, NullWritable, Void, Group> {
        private SimpleGroupFactory factory;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();

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

                    Group group = factory.newGroup()
                            .append("ip", ip)
                            .append("date", date)
                            .append("status", Integer.valueOf(status))
                            .append("method", method)
                            .append("url", url);



                    context.write(null,group);

                }
            }


        }
    }

    //利用正则表达式对数据进行预处理
    public static class PreMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());

            //System.out.println(rs);

        }
    }
}
