package com.lyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--05--15 14:58
 */
public class HbaseUtils {
    //声明hbase配置对象和连接对象
    public static Configuration conf = null;
    public static Connection conn = null;
    public static HBaseAdmin admin = null;
    static {
        //设置Hbase配置信息
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            //获取Hbase连接
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
        TableName tablename = TableName.valueOf(tableName);
        return admin.tableExists(tablename);
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param columnFamily 列族集合
     * @throws IOException
     */
    public static void createTable(String tableName, String...columnFamily) throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
        //判断表是否存在
        if (isTableExist(tableName)){
            System.out.println("表" + tableName + "已经存在");
        }else {
            TableName tablename = TableName.valueOf(tableName);
            //表描述对象建造者
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tablename);
            //列族描述对象建造者
            ColumnFamilyDescriptorBuilder cdb;
            //列族描述对象
            ColumnFamilyDescriptor cfd;
            for (String columnfamily : columnFamily) {
                cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnfamily));
                cfd = cdb.build();
                //将列族描述对象添加进表描述
                tdb.setColumnFamily(cfd);
            }
            //创建表描述对象
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
        }
    }

    /**
     * 删除表
     * @param tableName 表名
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
        TableName tablename = TableName.valueOf(tableName);
        if (isTableExist(tableName)){
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
            System.out.println("表" + tablename + "删除成功!");
        }else {
            System.out.println("表" + tablename + "不存在!");
        }
    }

    /**
     * 向表中插入一行数据
     * @param tableName 表名
     * @param rowKey   行键
     * @param columnFamily 列族
     * @param colunn 列限定符
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String colunn, String value) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        //获取表对象
        Table table = conn.getTable(tablename);
        //获取添加对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //向put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(colunn), Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("插入数据成功！");
    }

    /**
     *获取表所有数据
     * @param tableName 表名
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Table table = conn.getTable(tablename);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
//                //得到rowkey
//                System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
//                //得到列族
//                System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
//                //得到列限定符
//                System.out.println("列限定符：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//                //获取值
//                System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + " " + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + " " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

    }

    @After
    public static void close() throws IOException {
        conn.close();
        admin.close();
    }
}
