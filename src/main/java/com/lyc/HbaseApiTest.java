package com.lyc;

import org.junit.Test;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--05--15 14:58
 */
public class HbaseApiTest {

    @Test
    public void testIsTableExist() throws IOException {
        String tableName = "student";
        boolean tableExist = HbaseUtils.isTableExist(tableName);
        System.out.println(tableExist);
    }

    @Test
    public void testCreateTable() throws IOException {
        String tableName = "teacher";
        String[] columnFamily = {"info", "address"};
        HbaseUtils.createTable(tableName, columnFamily);
    }

    @Test
    public void testDropTable() throws IOException {
        String tableName = "teacher";
        HbaseUtils.dropTable(tableName);
    }

    @Test
    public void testAddRowDat() throws IOException {
        String tableName = "student";
        String rowKey = "1003";
        String columnFamily = "info";
        String column = "name";
        String value = "tom";
        HbaseUtils.addRowData(tableName, rowKey, columnFamily, column, value);
    }

    @Test
    public void testGetAllRows() throws IOException {
        String tableName = "student";
        HbaseUtils.getAllRows(tableName);
    }
}
