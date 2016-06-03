package org.dxer.hbase;

import org.apache.hadoop.hbase.client.Connection;

/**
 * 
 * @class HBaseTest
 * @author linghf
 * @version 1.0
 * @since 2016年3月29日
 */
public class HBaseTest {

    public static void main(String[] args) throws Exception {
        Connection connection = HBaseConnection.getConnection();
        System.out.println(HBaseUtil.existTable(connection, "user"));
    }
}
