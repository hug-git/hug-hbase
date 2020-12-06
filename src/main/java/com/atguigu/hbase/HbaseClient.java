package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseClient {
    Connection connection;

    @Before
    public void before() throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        connection = ConnectionFactory.createConnection(configuration);
    }

    @After
    public void after() throws IOException {
        connection.close();
    }

    @Test
    public void createTable() throws IOException {
        //建立主体对象admin
        Admin admin = connection.getAdmin();

        //用admin建表
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf("test:t_hello"));

        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes());

        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());

        admin.createTable(tableDescriptorBuilder.build());

        //关闭
        admin.close();
    }

    @Test
    public void existsTable() throws IOException {
        Admin admin = connection.getAdmin();

        boolean exists = admin.tableExists(TableName.valueOf("test:t_hello"));

        if (exists) {
            System.out.println("存在");
        } else {
            System.out.println("不存在");
        }

    }

    @Test
    public void createNamespace() throws IOException {
        Admin admin = connection.getAdmin();

        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("test1");
        admin.createNamespace(builder.build());

        admin.close();
    }

    @Test
    public void deleteTable() throws IOException {
        Admin admin = connection.getAdmin();

        admin.disableTable(TableName.valueOf("test:t_hello"));
        admin.deleteTable(TableName.valueOf("test:t_hello"));

        admin.close();
    }

    @Test
    public void deleteNamespace() throws IOException {
        Admin admin = connection.getAdmin();

        admin.deleteNamespace("test1");

        admin.close();
    }

    @Test
    public void put() throws IOException {
        //获取Table对象
        Table table = connection.getTable(TableName.valueOf("test:hello"));

        //put对象
        Put put = new Put("1001".getBytes());
        put.addColumn(
                "info".getBytes(),
                "name".getBytes(),
                "jack".getBytes()
        );
        put.addColumn(
                "info".getBytes(),
                "age".getBytes(),
                "19".getBytes()
        );
        put.addColumn(
                "info".getBytes(),
                "gender".getBytes(),
                "male".getBytes()
        );

        table.put(put);

        table.close();
    }

    @Test
    public void scan() throws IOException {
        Table table = connection.getTable(TableName.valueOf("test:hello"));

        Scan scan = new Scan();

        scan.withStartRow("1001".getBytes());
        scan.withStopRow("1002".getBytes());

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.write(CellUtil.cloneRow(cell));
                System.out.write(" ".getBytes());
                System.out.write(CellUtil.cloneFamily(cell));
                System.out.write(":".getBytes());
                System.out.write(CellUtil.cloneQualifier(cell));
                System.out.write(" ".getBytes());
                System.out.write(CellUtil.cloneValue(cell));
                System.out.write("\n".getBytes());
            }
        }
        table.close();
    }

    @Test
    public void get() throws IOException {
        Table table = connection.getTable(TableName.valueOf("test:hello"));

        Get get = new Get("1001".getBytes());

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.write(CellUtil.cloneRow(cell));
            System.out.write(" ".getBytes());
            System.out.write(CellUtil.cloneFamily(cell));
            System.out.write(":".getBytes());
            System.out.write(CellUtil.cloneQualifier(cell));
            System.out.write(" ".getBytes());
            System.out.write(CellUtil.cloneValue(cell));
            System.out.write("\n".getBytes());
        }
    }

    @Test
    public void get2() throws IOException {
        Table table = connection.getTable(TableName.valueOf("test:hello"));

        Get get = new Get("1001".getBytes());

        get.addColumn(
                "info".getBytes(),
                "name".getBytes()
        );
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.write(CellUtil.cloneRow(cell));
            System.out.write(" ".getBytes());
            System.out.write(CellUtil.cloneFamily(cell));
            System.out.write(":".getBytes());
            System.out.write(CellUtil.cloneQualifier(cell));
            System.out.write(" ".getBytes());
            System.out.write(CellUtil.cloneValue(cell));
            System.out.write("\n".getBytes());
        }
    }

    @Test
    public void delete() throws IOException {
        Table table = connection.getTable(TableName.valueOf("test:hello"));
        Delete delete = new Delete("1001".getBytes());

        delete.addColumn(
                "info".getBytes(),
                "name".getBytes()
        );

        table.delete(delete);

        table.close();
    }

    @Test
    public void truncate() throws IOException {
        Admin admin = connection.getAdmin();

        admin.disableTable(TableName.valueOf("test:hello"));
        admin.truncateTable(TableName.valueOf("test:hello"), true);

        admin.close();
    }
}
