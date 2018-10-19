package com.day1;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

public class HdfsCURD {
    public static void main(String[] args) throws IOException {
        //testCreate();
        //testRead();
        //testPut();
        testDelete();
        //testDownload();
    }

    private static void testDownload() throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        Path path = new Path("/a.txt");
        Path file=new Path("E://baidupan//test");
        fileSystem.copyToLocalFile(path,file);
        fileSystem.close();
    }

    private static void testDelete() throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        Path path = new Path("/a.txt");
        fileSystem.delete(path,true);
        fileSystem.close();
    }


    private static void testPut() throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        Path path = new Path("/");
        Path file=new Path("E://baidupan//a.txt");
        /*String[] test = file.list();
        for(int i=0;i<test.length;i++)
            fileSystem.copyFromLocalFile(new Path("E://baidupan//a//"+test[i]),path);*/
        fileSystem.copyFromLocalFile(file,path);
        fileSystem.close();

    }

    private static void testRead() throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        List<String> returnValue = new ArrayList<String>();
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        Path path = new Path(("/java1"));
        FSDataInputStream inputStream = fileSystem.open(path);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        LineNumberReader lineNumberReader = new LineNumberReader(inputStreamReader);
        String str = null;
        while((str = lineNumberReader.readLine())!=null)
            returnValue.add(str);
        for (String a:returnValue) {
            System.out.println(a);
        }
        lineNumberReader.close();
        inputStreamReader.close();
        inputStream.close();
        fileSystem.close();

    }

    private static void testCreate() throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/java1"));
        fsDataOutputStream.write("hello".getBytes());
        fsDataOutputStream.close();
        fileSystem.close();
    }
}
