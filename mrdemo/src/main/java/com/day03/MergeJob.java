package com.day03;

import com.google.common.io.Resources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
*@ClassName:DistinctJob
 @Description:TODO
 @Author:
 @Date:2018/10/10 14:04 
 @Version:v1.0
*/

/*
*
要求：
1.扫描hdfs上的文本文件
2.小文件的判定标准是小于2MB
3,合并小文件成一个大文件。
合并的方式是每个小文件的内容为一行。如果小文件为多行文件，则将小文件拼成字符串。
一行由key:filename,value：content组成。
如：
a.txt  的内容：
hello henan
hello world
b.txt  的内容：
hello henan
hello zhengzhou

结果：
a.txt:hello henan hello world
b.txt:hello henan hello zhengzhou
* */

public class MergeJob {

    public static class MergeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        }


    }
}
