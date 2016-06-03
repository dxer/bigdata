package org.dxer.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * 
 * @class HBaseConnection
 * @author linghf
 * @version 1.0
 * @since 2016年3月29日
 */
public class HBaseConnection {

    public static Connection getConnection() throws Exception {
        Configuration conf = HBaseConfig.getConfiguration();

        Connection connection = ConnectionFactory.createConnection(conf);

        return connection;
    }
}
