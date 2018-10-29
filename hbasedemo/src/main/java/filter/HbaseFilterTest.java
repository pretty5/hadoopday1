package filter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
/*
*@ClassName:HbaseFilterTest
 @Description:TODO
 @Author:
 @Date:2018/10/26 9:45 
 @Version:v1.0
*/

public class HbaseFilterTest {
    private Configuration conf = null;
    private Connection con = null;
    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master:2181");
        con=ConnectionFactory.createConnection(conf);
    }
    @Test
    public void testRowFilter() throws IOException {
        //select from stu where id>1
        //第一个是比较方式，EQUAL 相等，第二个是rowkey的值
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("row1".getBytes()));

        Scan scan = new Scan();

        scan.setFilter(rowFilter);

        Table table = con.getTable(TableName.valueOf("t1"));

        //Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {
            //第一个是列蔟，第二个列名
            String r = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("a")));

            System.out.println(r);
        }
        table.close();

    }
    @Test
    public void testCombineFilter() throws IOException {
        //select * from stu where id>1
        //select sex from peron where row = bbb;

        QualifierFilter qf = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("a".getBytes()));
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("row1".getBytes()));

        Scan scan = new Scan();

       // ArrayList<FilterBase> list = new ArrayList<FilterBase>();
        FilterList filterList = new FilterList();
        filterList.addFilter(rowFilter);
        filterList.addFilter(qf);
      /*  list.add(rowFilter);
        list.add(qf);*/
      scan.setFilter(filterList);

       /* scan.setFilter(rowFilter);
        scan.setFilter(qf);*/

        Table table = con.getTable(TableName.valueOf("t1"));

        //Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {

            String r = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("a")));

            System.out.println(r);
        }
        table.close();

    }
    @Test
    public void testQualifierFilter() throws IOException {
        //select * from stu where id>1
        //select sex from stu;
        //第一个比较，相等 第二个是列名
        QualifierFilter qf = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("b".getBytes()));

        Scan scan = new Scan();

        scan.setFilter(qf);

        Table table = con.getTable(TableName.valueOf("t1"));

        //Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {
            //1.列族，2.列名
            String r = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("b")));

            System.out.println(r);
        }
        table.close();

    }
    @Test
    public void testRowFilterGreater() throws IOException {
        //select from stu where id>1

        //第一个GREATER，大于
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator("row23".getBytes()));

        Scan scan = new Scan();

        scan.setFilter(rowFilter);

        Table table = con.getTable(TableName.valueOf("t1"));

        //Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {

            String de = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("a")));
            String sex = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("d")));
            System.out.println(de);
            System.out.println(sex);
        }
        table.close();
    }

    @Test
    public void testPageFilter() throws IOException {
        //select from stu where id>1
        //第一个是比较方式，EQUAL 相等，第二个是rowkey的值
        PageFilter pageFilter = new PageFilter(1);
        //FilterList filterList = new FilterList();
        //RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("row1".getBytes()));
       // filterList.addFilter(pageFilter);
       // filterList.addFilter(rowFilter);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("a"));
        scan.setStartRow(Bytes.toBytes(1));
        scan.setFilter(pageFilter);
        Table table = con.getTable(TableName.valueOf("t1"));

        //Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {

            //第一个是列蔟，第二个列名
            String r = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("a")));
            String b = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("b")));
            System.out.println(b);
            System.out.println(r);
        }
        table.close();

    }

    @Test
    public void testValueFilter() throws IOException {
        //select * from stu where id>1
        //select sex from stu;
        //第一个比较，相等 第二个是列名
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("1"));

        Scan scan = new Scan();

        scan.setFilter(valueFilter);

        Table table = con.getTable(TableName.valueOf("t1"));

        //Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);

        for(Result result : scanner) {
            //1.列族，2.列名
           /* String r = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c")));
            System.out.println(r);*/

            byte[] value = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.out.println(new String(value));
        }


        table.close();

    }

}
