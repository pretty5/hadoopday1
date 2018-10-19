package com.day04;

import com.day03.MaxSaleJob;
import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
*@ClassName:MergeFileJob
 @Description:TODO
 @Author:
 @Date:2018/10/12 9:28 
 @Version:v1.0
*/
public class MergeFileJob {


    public static class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
        Path filePath = null;
        Boolean flag = false;
        //@Override
        /*protected void setup(Context context) throws IOException, InterruptedException {
            //super.setup(context);
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            filePath = inputSplit.getPath();
            Configuration configuration = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            //过滤目录
            if (fileSystem.getFileStatus(filePath).isDirectory()){
                flag=true;
                return;
            }
            //获取文件的状态
            long len = fileSystem.getFileStatus(filePath).getLen();
            //fileSystem.close();
            if (len>2*1024*1024||!filePath.getName().contains("txt")){
                //如果文件小于两兆则运行
               //continue;
                flag=true;
            }

        }*/

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            context.write(new Text(inputSplit.getPath().getName()), value);


        }
    }

    public static class MergeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //super.reduce(key, values, context);
            StringBuilder content = new StringBuilder();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text line = iterator.next();
                content.append(line.toString()).append(" ");

            }
            context.write(key, new Text(content.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        //coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "merge");
        //设置job的运行类
        job.setJarByClass(MergeFileJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReducer.class);

        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileSystem fileSystem = FileSystem.get(coreSiteConf);


        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            if (!fileStatus.isDirectory()) {
                //判断大小 及格式
                if (fileStatus.getLen() < 2 * 1014 * 1024 && fileStatus.getPath().getName().contains("txt")) {

                    FileInputFormat.addInputPath(job, fileStatus.getPath());
                }
            }
        }


        //设置任务的输入路径

        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job, new Path("/merge1/"));
        //运行任务
        boolean flag = job.waitForCompletion(true);

    }


}
