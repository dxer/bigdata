package org.dxer.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @class HBaseUtil
 * @author linghf
 * @version 1.0
 * @since 2016年3月29日
 */
public class HBaseUtil {

    private static boolean verify(Connection connection, String tableName) {
        if (connection != null && tableName != null && tableName.length() > 0) {
            return true;
        }
        return false;
    }

    /**
     * 表是否存在
     * 
     * @param connection
     * @param tableName
     * @return
     */
    public static boolean existTable(Connection connection, String tableName) {
        if (!verify(connection, tableName)) {
            return false;
        }

        boolean isExists = false;
        Admin admin = null;
        try {
            admin = (HBaseAdmin) connection.getAdmin();
            isExists = admin.tableExists(TableName.valueOf(tableName));
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                }
            }
        }
        return isExists;
    }

    /**
     * 获取table对象
     * 
     * @param connection
     * @param tableName
     * @return
     */
    public static Table getHtable(Connection connection, String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 获取数据
     * 
     * @param connection
     * @param tableName
     * @param rowkey
     * @return
     */
    public static Result get(Connection connection, String tableName, String rowkey) {
        Result result = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            if (table != null) {
                Get get = new Get(Bytes.toBytes(rowkey));
                result = table.get(get);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                }
            }
        }
        return result;
    }

    /**
     * 记录是否存在
     * 
     * @param connection
     * @param tableName
     * @param rowkey
     * @return
     */
    public static boolean isRecordExist(Connection connection, String tableName, String rowkey) {
        boolean ret = false;
        Result result = get(connection, tableName, rowkey);
        if (result != null && !result.isEmpty()) {
            ret = true;
        }
        return ret;
    }

    /**
     * 获取数据
     * 
     * @param connection
     * @param tableName
     * @param rowkey
     * @param family
     * @param cols
     * @return
     */
    public static Result get(Connection connection, String tableName, String rowkey, String family, String... cols) {
        Result result = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            if (table != null) {
                Get get = new Get(Bytes.toBytes(rowkey));

                if (family != null && family.length() > 0 && cols != null && cols.length > 0) {
                    for (String col: cols) {
                        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                    }
                }

                result = table.get(get);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                }
            }
        }

        return result;
    }

    /**
     * 插入数据
     * 
     * @param connection
     * @param tableName
     * @param puts
     */
    public static void put(Connection connection, String tableName, Put... puts) {
        if (verify(connection, tableName) && puts != null && puts.length > 0) {
            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                if (table != null) {
                    table.put(Arrays.asList(puts));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (table != null) {
                    try {
                        table.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
    }

    /**
     * 插入数据
     * 
     * @param connection
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param columnName
     * @param columnValue
     */
    public static void put(Connection connection, String tableName, String rowkey, String columnFamily,
                           String columnName, String columnValue) {
        if (verify(connection, tableName)) {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));

            put(connection, tableName, put);
        }
    }

    /**
     * 插入数据
     * 
     * @param connection
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param values
     */
    public static void put(Connection connection, String tableName, String rowkey, String columnFamily,
                           Map<String, String> values) {
        if (verify(connection, tableName)) {
            Put put = new Put(Bytes.toBytes(rowkey));
            if (values != null && !values.isEmpty()) {
                byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
                for (String name: values.keySet()) {
                    String columnValue = values.get(name);
                    put.addColumn(columnFamilyBytes, Bytes.toBytes(name), Bytes.toBytes(columnValue));
                }

                put(connection, tableName, put);
            }
        }
    }

    /**
     * 查案如数据
     * 
     * @param connection
     * @param tableName
     * @param rowkey
     * @param params
     */
    public static void put(Connection connection, String tableName, String rowkey,
                           Map<String, Map<String, String>> params) {
        if (verify(connection, tableName)) {
            Put put = new Put(Bytes.toBytes(rowkey));
            if (params != null && !params.isEmpty()) {
                for (String cf: params.keySet()) {
                    Map<String, String> cols = params.get(cf);
                    if (cf != null && cf.length() > 0 && cols != null && !cols.isEmpty()) {
                        byte[] cfBytes = Bytes.toBytes(cf);
                        for (String col: cols.keySet()) {
                            String value = cols.get(col);
                            put.addColumn(cfBytes, Bytes.toBytes(col), Bytes.toBytes(value));
                        }
                    }
                }
                put(connection, tableName, put);
            }
        }
    }

    /**
     * 删除数据
     * 
     * @param connection
     * @param tableName
     * @param rowkey
     */
    public static void delete(Connection connection, String tableName, String rowkey) {
        if (verify(connection, tableName)) {
            Table table = null;
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                if (table != null) {
                    Delete del = new Delete(Bytes.toBytes(rowkey));
                    table.delete(del);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (table != null) {
                    try {
                        table.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
    }

    /* 转换byte数组 */
    private static byte[] getBytes(String str) {
        if (str == null)
            str = "";

        return Bytes.toBytes(str);
    }

    public static PageResult getRange(Connection connection, String tableName, String startRow, String stopRow,
                                      Integer currentPage, Integer pageSize, Map<String, Map<String, String>> params) {
        return getRange(connection, tableName, startRow, stopRow, currentPage, pageSize, params, null);
    }

    /**
     * 获取数据
     * 
     * @param connection
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param currentPage
     * @param pageSize
     * @param params
     * @param resultColumn
     * @return
     */
    public static PageResult getRange(Connection connection, String tableName, String startRow, String stopRow,
                                      Integer currentPage, Integer pageSize, Map<String, Map<String, String>> params,
                                      Map<String, List<String>> resultColumn) {
        ResultScanner scanner = null;
        PageResult pageResult = null;
        try {
            // 获取最大返回结果数量
            if (pageSize == null || pageSize == 0L)
                pageSize = 100;

            if (currentPage == null || currentPage == 0)
                currentPage = 1;

            // 计算起始页和结束页
            Integer firstPage = (currentPage - 1) * pageSize;

            Integer endPage = firstPage + pageSize;

            Table table = connection.getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();
            scan.setStartRow(getBytes(startRow));
            scan.setStopRow(getBytes(stopRow));
            scan.setCaching(1000);
            scan.setCacheBlocks(false);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            if (params != null && params.size() > 0) {
                for (String cf: params.keySet()) {
                    Map<String, String> cMap = params.get(cf);
                    if (cMap != null && cMap.size() > 0) {
                        for (String col: cMap.keySet()) {
                            String value = cMap.get(col);

                            SingleColumnValueFilter filter = new SingleColumnValueFilter(getBytes(cf), getBytes(col),
                                            CompareFilter.CompareOp.EQUAL, getBytes(value));
                            filter.setFilterIfMissing(true); //

                            scan.addColumn(getBytes(cf), getBytes(col));
                            filterList.addFilter(filter);
                        }
                    }
                }
            }

            scanner = table.getScanner(scan);
            Result result = null;
            List<byte[]> rowList = new LinkedList<byte[]>();
            int count = 0;
            while ((result = scanner.next()) != null) {
                if (count >= firstPage && count < endPage) {
                    rowList.add(result.getRow());
                }
                count++;
            }

            List<Get> gets = getList(rowList, resultColumn);

            Result[] results = table.get(gets);

            pageResult = new PageResult();
            pageResult.setCurrentPage(currentPage);
            pageResult.setPageSize(pageSize);
            pageResult.setTotalCount(count);
            pageResult.setTotalPage(getTotalPage(pageSize, count));
            pageResult.setResults(results);

            return pageResult;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return null;
    }

    private static int getTotalPage(int pageSize, int totalCount) {
        int n = totalCount / pageSize;
        if (totalCount % pageSize == 0) {
            return n;
        } else {
            return ((int) n) + 1;
        }
    }

    /* 根据ROW KEY集合获取GET对象集合 */
    private static List<Get> getList(List<byte[]> rowList, Map<String, List<String>> resultColumn) {
        List<Get> list = new LinkedList<Get>();

        for (byte[] row: rowList) {
            Get get = new Get(row);

            if (resultColumn != null && resultColumn.size() > 0) {
                for (String cf: resultColumn.keySet()) {
                    List<String> cols = resultColumn.get(cf);
                    if (cols != null && cols.size() > 0) {
                        for (String col: cols)
                            get.addColumn(getBytes(cf), getBytes(col));
                    }
                }
            }

            list.add(get);
        }
        return list;
    }
}
