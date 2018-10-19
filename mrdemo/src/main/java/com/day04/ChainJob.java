package com.day04;

import com.day01.*;
import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/*
*@ClassName:ChainJob
 @Description:TODO
 @Author:
 @Date:2018/10/12 11:02 
 @Version:v1.0
*/


/*
* 设置job依赖
* */
public class ChainJob {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        Job job1 = setJob1();
        Job job2 = setJob2();
        ControlledJob controlledJob1 = new ControlledJob(coreSiteConf);
        controlledJob1.setJob(job1);

        ControlledJob controlledJob2 = new ControlledJob(coreSiteConf);
        controlledJob2.setJob(job2);

        controlledJob2.addDependingJob(controlledJob1);

        JobControl jobControl = new JobControl("demo");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        new Thread(jobControl).start();

        while (true){
            List<ControlledJob> jobList = jobControl.getRunningJobList();
            System.out.println(jobList);
            Thread.sleep(5000);
        }


    }

    private static Job setJob2() throws IOException {
        Configuration coreSiteConf = new Configuration();
        //coreSiteConf.set("","");

        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "wcoo");
        //设置job的运行类
        job.setJarByClass(WCJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCCombiner.class);
        job.setReducerClass(WCReducer.class);
        job.setNumReduceTasks(2);
        job.setPartitionerClass(WCPartitioner.class);
        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/merge8/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job, new Path("/wcout900"));
        return job;

    }

    private static Job setJob1() throws IOException {
        Configuration coreSiteConf = new Configuration();

        //coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //设置一个任务
        Job job1 = Job.getInstance(coreSiteConf, "merge");
        //设置job的运行类
        job1.setJarByClass(MergeFileJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job1.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job1.setMapperClass(MergeFileJob.MergeMapper.class);
        job1.setReducerClass(MergeFileJob.MergeReducer.class);

        //map输出类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        FileSystem fileSystem = FileSystem.get(coreSiteConf);


        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            if (!fileStatus.isDirectory()) {
                //判断大小 及格式
                if (fileStatus.getLen() < 2 * 1014 * 1024 && fileStatus.getPath().getName().contains("txt")) {

                    FileInputFormat.addInputPath(job1, fileStatus.getPath());
                }
            }
        }


        //设置任务的输入路径

        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job1, new Path("/merge8/"));
        //运行任务
        return job1;
    }


}
