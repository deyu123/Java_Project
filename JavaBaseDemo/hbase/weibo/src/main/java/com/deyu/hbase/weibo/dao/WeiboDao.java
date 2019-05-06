package com.deyu.hbase.weibo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiboDao {

    public static Connection connection = null;

    public synchronized Connection getConnection() throws IOException {
        if (connection == null) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop202,hadoop203,hadoop204");
            connection = ConnectionFactory.createConnection(conf);
        }
        return connection;
    }

    /**
     * 创建NameSpace
     *
     * @param nameSpace
     * @throws IOException
     */
    public void createNameSpace(String nameSpace) throws IOException {

        Admin admin = getConnection().getAdmin();
        NamespaceDescriptor weibo = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(weibo);
        admin.close();
    }


    /**
     * 创建表
     *
     * @param tableName
     * @param families
     * @throws IOException
     */
    public void createTable(String tableName, String... families) throws IOException {
        createTable(tableName, 1, families);
    }

    public void createTable(String tableName, int versions, String... families) throws IOException {
        Admin admin = getConnection().getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        for (String family : families) {
            HColumnDescriptor cf = new HColumnDescriptor(family);
            cf.setMaxVersions(versions);
            table.addFamily(cf);
        }
        admin.createTable(table);
        admin.close();
    }

    /**
     * 插入一个cell
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public List<String> getAllRowKeysByPrefix(String tableName, String prefix) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
        ResultScanner scanner = table.getScanner(scan);

        List<String> list = new ArrayList<>();

        for (Result result : scanner) {
            byte[] row = result.getRow();
            list.add(Bytes.toString(row));
        }
        table.close();

        return list;
    }


    /**
     * 往多行相同的列中插入相同的数据
     *
     * @param tableName
     * @param rowKeys
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public void putCell(String tableName, List<String> rowKeys, String family, String column, String value) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();
        for (String rowKey : rowKeys) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            puts.add(put);
        }
        table.put(puts);

        table.close();
    }

    /**
     * 删除一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void deleteRow(String tableName, String rowKey) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除一个cell
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public void deleteCell(String tableName, String rowKey, String family, String column) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));
        table.delete(delete);
        table.close();
    }

    /**
     * 获取多行的相同列
     *
     * @param tableName
     * @param prefix
     * @param family
     * @param column
     * @return
     * @throws IOException
     */
    public List<String> getCellsByPrefix(String tableName, String prefix, String family, String column) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
        ResultScanner scanner = table.getScanner(scan);

        table.close();
        List<String> list = new ArrayList<String>();

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                list.add(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return list;
    }

    /**
     * 获取一行中的一个列族下的所有列的所有版本数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     * @throws IOException
     */
    public List<String> getCellsByRowKeyAndFamily(String tableName, String rowKey, String family) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(family));
        get.setMaxVersions();
        Result result = table.get(get);

        table.close();
        List<String> list = new ArrayList<>();

        for (Cell cell : result.rawCells()) {
            list.add(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        return list;
    }

    /**
     * 获取一行中的一列数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @return
     * @throws IOException
     */
    public String getCell(String tableName, String rowKey, String family, String column) throws IOException {

        Table table = getConnection().getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        Result result = table.get(get);

        String content = null;
        for (Cell cell : result.rawCells()) {
            content = Bytes.toString(CellUtil.cloneValue(cell));
        }
        return content;
    }
}