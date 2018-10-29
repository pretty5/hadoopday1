package teacherTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseCURD {
    public static void main(String[] args) throws IOException {
        HbaseCURD hbaseCURD = new HbaseCURD();
        hbaseCURD.init();
       // hbaseCURD.testCreateTable();
        hbaseCURD.testPut("people");

    }

    private Configuration conf = null;
    private Connection con = null;
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.18.3:2181");
    }

    public void testCreateTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("people"));
        HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
        HColumnDescriptor hcdData = new HColumnDescriptor("data");
        hcdInfo.setMaxVersions(3);
        htd.addFamily(hcdInfo);
        htd.addFamily(hcdData);
        admin.createTable(htd);
        //用完关闭
        admin.close();
    }
    /**插入数据
     * @throws IOException **/

    public void testPut(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes("row001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lingxin"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("57"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000));
        table.put(put);
        table.close();
    }
    /**测试插入100万条数据**/

    public void testPutAll() throws IOException {
        HTable table = new HTable(conf, "people");
        List<Put> puts = new ArrayList<Put>(10000);
        for(int i = 1; i < 1000000; i++) {
            Put put = new Put(Bytes.toBytes("row" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lingxin" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("57"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000));
            puts.add(put);
            //每隔10000,写一次
            if(i % 10000 == 0) {
                table.put(puts);
                puts = new ArrayList<Put>(10000);
            }
        }
        table.put(puts);
        table.close();
        //以下方式不可取
        /*      for(int i=1; i<1000000; i++) {
                    Put put = new Put(Bytes.toBytes("row"+i));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lingxin"+i));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("57"));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000));
                    puts.add(put);
                }
                table.put(puts);
                table.close();*/
    }
    /**查看某个cell的值**/

    public void testGet() throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));
        Get get = new Get(Bytes.toBytes("row9999"));
        Result resut = table.get(get);
        String r = Bytes.toString(resut.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
        System.out.println(r);
        table.close();
    }
    /**查看某个rowkey范围的数据，按字典顺序排序**/

    public void testScan() throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));
        Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner) {
            String r = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(r);
        }
        table.close();
    }
    /**组合使用扫描器缓存和批量大小**/

    public void testScanWithCacheAndBatch(int caching, int batch) throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));
        Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        scan.setCaching(caching);
        scan.setBatch(batch);
        //!!!!!!!!!!!!!!!!!!!!!!!!!!
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner) {
            String r = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(r);
        }
        table.close();
    }

    public void testDel() throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));//	HTable table = new HTable(conf, "people");
        Delete delete = new Delete(Bytes.toBytes("row9999"));
        table.delete(delete);
        table.close();

    }


}
