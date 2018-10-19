package com.day01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPartitioner extends Partitioner<Text,IntWritable> {
    public int getPartition(Text key, IntWritable intWritable, int i) {
        String s = key.toString().substring(0, 1).toLowerCase();
        if (s.compareTo("m")>0) {
            return 0;
        }else {
            return 1;
        }
    }
}
