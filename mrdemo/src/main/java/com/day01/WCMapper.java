package com.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
*@ClassName:WCMapper
 @Description:TODO
 @Author:
 @Date:2018/10/9 15:20 
 @Version:v1.0
*/
//用来处理map任务
//map任务接受的kv,输出的也是kv

//第一个泛型表示：输入key的数据类型 输入的数据相对于文件开头的偏移量（行号）
//第二个泛型表示：输入value的数据类型  是输入的文件的一行内容
//第三个泛型表示：输出key的数据类型
//第四个泛型表示：输出value的数据类型

//LongWritable 等价于java中long
//Text 等价于java中的string
//Intwritable 等价于java中的int

//**Writable 是hadoop定义的基本数据类型，相当于对java中的数据类型做了一个封装，同时支持序列化

public class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    //map方法每次处理一行数据 会被循环调用
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        IntWritable one = new IntWritable(1);
        //遍历单词 输出  word 1
        for (int i = 0; i < words.length; i++) {
            Text keyOut = new Text(words[i]);
            //输出
            context.write(keyOut,one);
        }
    }
}
