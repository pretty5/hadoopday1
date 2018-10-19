package com.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/*
*@ClassName:WCReducer
 @Description:TODO
 @Author:
 @Date:2018/10/9 15:45 
 @Version:v1.0
*/
//用来处理reduce任务
//在reduce端 框架会将相同的key的value放在一个集合中
public class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    //reduce每次处理一个key  会被循环调用
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int count=0;
        while (iterator.hasNext()){
            IntWritable one = iterator.next();
            count+=one.get();
        }
        //context的write只接受hadoop数据类型，不能输出java的数据类型
        context.write(key,new IntWritable(count));
    }
}
