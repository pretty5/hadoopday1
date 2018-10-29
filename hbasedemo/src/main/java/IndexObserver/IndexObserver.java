package IndexObserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;

/*
*@ClassName:IndexObserver
 @Description:TODO
 @Author:
 @Date:2018/10/26 14:27 
 @Version:v1.0
*/

/*
* 二级索引
* */
public class IndexObserver extends BaseRegionObserver {
    //stu i_stu
    //rowkey name age
    //i_age  age:rowkey
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //super.prePut(e, put, edit, durability);
        byte[] rowkey = put.getRow();
        //byte[] age = put.getAttribute("age");
        List<Cell> cells = put.get("info".getBytes(), "age".getBytes());
        for (Cell cell :
                cells) {
            byte[] age = cell.getValue();
            TableName tableName = e.getEnvironment().getRegion().getTableDesc().getTableName();
            HTableInterface table = e.getEnvironment().getTable(tableName);
            Put indexPut = new Put(age).addColumn("info".getBytes(), "i_age_rowkey".getBytes(), rowkey);
            table.put(indexPut);
        }
    }
}
