package myTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;

public class HBaseDemo {
    private Connection conn;
    private Table table;
    private Admin admin;

    public static void main(String[] args) throws IOException, DeserializationException {
        HBaseDemo demo=new HBaseDemo();
        //demo.createNS("test2");
        //demo.createTbl("test2","teacherTest","baseinfo",5,"100000","909090",5);
        //demo.put("test2","teacherTest","222222","baseinfo","name","Kevin",1000);
        //demo.get("test2","teacherTest","222222999","baseinfo","name");
        demo.scan(null,"test1");
        //demo.scan("test2","teacherTest",new KeyOnlyFilter(true));
        //demo.scan("test2","teacherTest",new RowFilter(CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("222222100"))));
        // 测试协处理器
        //demo.get("test2","teacherTest","222222950");
    }


    public HBaseDemo() throws IOException{
        System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-2.7.5");
        String home = System.getProperty("hadoop.home.dir");
        // 0.先获取HadoopAPI中的Configuration对象
        Configuration hconf=new Configuration();
        // 1.获取HBaseConfiguration对象
        Configuration conf=
                HBaseConfiguration.create(hconf);
        // 2.获取Connection对象
        conn=ConnectionFactory.createConnection(conf);
        // 3.获取Admin对象
        admin=conn.getAdmin();
    }

    public HBaseDemo(String tbl) throws IOException{
        this();
        // 4.获取Table对象
        table=conn.getTable(TableName.valueOf(tbl));
    }

    public void scan(String ns,String tbl) throws IOException {
        // 获取表对象
        Table table=getTbl(ns,tbl);
        // 获取Scanner对象
        Scan scan=new Scan();
        // 通过Scan对象获取Scanner对象
        ResultScanner scanner=table.getScanner(scan);
        // 获取迭代器对象
        Iterator<Result> iterator=
                scanner.iterator();
        // 进行迭代，拿到Result对象
        while(iterator.hasNext()){
            Result result=iterator.next();
            showResult(result);
        }
    }
    public void scan(String ns,String tbl,Filter filter) throws IOException{
        Table table=getTbl(ns,tbl);
        // 获取Scan对象，并且加入过滤器，
        Scan scan=new Scan();
        scan.setFilter(filter);
        Iterator<Result> iterator=
                table.getScanner(scan).iterator();
        while(iterator.hasNext()){
            showResult(iterator.next());
        }
    }


    public void showResult(Result result){
        // 通过Result对象获取行键
        byte[] row=result.getRow();
        // 获取Map对象
        NavigableMap<byte[],
                NavigableMap<byte[],
                        NavigableMap<Long,byte[]>>>
                map=result.getMap();
        // map中的键是什么？   -->     列族
        // map中的值是什么？   -->     某个列族对应的值
        // 遍历map
        Set<byte[]> cfs=map.keySet();
        for(byte[] cf : cfs){
            NavigableMap<byte[],
                    NavigableMap<Long,byte[]>>
                    map1=map.get(cf);
            // map1中的键是什么？  -->   列名
            // map1中的值是什么？  -->   某个列对应的值
            // 遍历map1
            Set<byte[]> cols=map1.keySet();
            for(byte[] col : cols){
                NavigableMap<Long,byte[]>
                        map2=map1.get(col);
                // map2中的键是什么？  -->   Timestamp
                // map2中的值是什么？  -->   值
                // 遍历map2
                Set<Long> tss=map2.keySet();
                for(Long ts : tss){
                    byte[] val=map2.get(ts);
                    System.out.println(
                            "行键："+new String(row)+
                                    "\t列族："+new String(cf)+
                                    "\t列名："+new String(col)+
                                    "\t时间戳："+ts+
                                    "\t值："+new String(val));
                }
            }
        }
    }

    // 封装方法，用来获取表对象
    public Table getTbl(String ns,String tbl) throws IOException{
        return conn.getTable(
                TableName.valueOf(
                        ns==null||"".equals(ns)?
                                tbl:ns+":"+tbl));
    }

    // 1.创建命名空间
    public void createNS(String ns) throws IOException{
        // 创建命名空间描述器对象
        NamespaceDescriptor nsDesc=
                NamespaceDescriptor.
                        create(ns).build();
        admin.createNamespace(nsDesc);
    }

    /**
     * 2.创建表
     * @param ns    命名空间
     * @param tbl   表名
     * @param cf    列族名
     * @param vers  某一列的版本数
     * @param nums  对该表进行分区的个数
     * @param sk    Region的行键的起始值
     * @param ek    Region的行键的结束值
     */
    public void createTbl(
            String ns,
            String tbl,
            String cf,
            int vers,
            String sk,
            String ek,
            int nums) throws IOException{
        // 构建表描述器
        HTableDescriptor tblDesc=
                new HTableDescriptor(
                        TableName.valueOf(
                                ns==null||"".equals(ns)?
                                        tbl:ns+":"+tbl));
        // 在创建表的时候需要传递列族，创建HColumnDescriptor
        HColumnDescriptor hcolDesc=
                new HColumnDescriptor(cf);
        // 使用HTableDescriptor对象的addFamily方法，
        // 将列族添加到表上
        tblDesc.addFamily(hcolDesc);
        // 给某一列设置版本数
        hcolDesc.setMaxVersions(vers>1?vers:1);
        // 创建表
        admin.createTable(
                tblDesc,
                Bytes.toBytes(sk),
                Bytes.toBytes(ek),
                nums);
    }

    /**
     *
     * @param ns    命名空间
     * @param tbl   表名
     * @param rk    行键
     * @param cf    列族名
     * @param col   列名
     * @param val   要插入的值
     */
    public void put(
            String ns,String tbl,String rk,
            String cf,String col,String val) throws IOException{
        // 获取表对象
        Table table=getTbl(ns,tbl);
        // 构建Put对象，参数是行键 RowKey
        Put put=new Put(Bytes.toBytes(rk));
        put.addColumn(
                Bytes.toBytes(cf),      // 列族
                Bytes.toBytes(col),     // 列名
                Bytes.toBytes(val));    // 值
        // 插入数据
        table.put(put);
    }

    public void put(
            String ns,String tbl,String rk,
            String cf,String col,String val,int max) throws IOException{
        // 获取表对象
        Table table=getTbl(ns,tbl);
        for(int x=0;x<max;x++){
            // 构建Put对象，参数是行键 RowKey
            Put put=new Put(Bytes.toBytes(rk+x));
            put.addColumn(
                    Bytes.toBytes(cf),      // 列族
                    Bytes.toBytes(col),     // 列名
                    Bytes.toBytes(val+x));    // 值
            // 插入数据
            table.put(put);
        }
    }

    // 3.获取值
    public void get(String ns,String tbl,String rk) throws IOException{
        // 获取表对象
        Table table=getTbl(ns,tbl);
        // 获取值
        Get get=new Get(Bytes.toBytes(rk));
        Result result=
                table.get(get);
        showResult(result);
    }



    public Connection getConn(){
        return conn;
    }

    public void setConn(Connection conn){
        this.conn=conn;
    }

    public Table getTable(){
        return table;
    }

    public void setTable(Table table){
        this.table=table;
    }

    public Admin getAdmin(){
        return admin;
    }

    public void setAdmin(Admin admin){
        this.admin=admin;
    }
}

