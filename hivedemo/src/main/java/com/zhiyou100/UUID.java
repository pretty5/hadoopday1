package com.zhiyou100;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UUID extends UDF {
    public String evaluate(){
        return  java.util.UUID.randomUUID().toString();
    }
}
