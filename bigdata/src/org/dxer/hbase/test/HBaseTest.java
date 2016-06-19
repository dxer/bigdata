package org.dxer.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {

	private HTable table = null;

	class UserInfo {
		private Long userId;

		private String name;

		private Integer age;

		public Long getUserId() {
			return userId;
		}

		public void setUserId(Long userId) {
			this.userId = userId;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Integer getAge() {
			return age;
		}

		public void setAge(Integer age) {
			this.age = age;
		}

	}

	public void addUserInfo(Long userId, String name, int age) throws IOException {

		String rowkey = new StringBuffer(userId + "").reverse().toString();
		Put put = new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
		table.put(put);
	}

	public void addUserInfos(List<UserInfo> userInfos) throws IOException {
		List<Put> puts = new ArrayList<Put>();
		if (userInfos != null) {
			for (UserInfo userInfo : userInfos) {

			}
		}

		if (puts != null && puts.size() > 0) {
			table.put(puts);
		}
	}

	public static void main(String[] args) {
		Configuration config = HBaseConfiguration.create();
		// 配置hbase.zookeeper.quorum: 后接zookeeper集群的机器列表
		config.set("hbase.zookeeper.quorum", "192.168.1.100");
		// 配置hbase.zookeeper.property.clientPort: zookeeper集群的服务端口
		config.set("hbase.zookeeper.property.clientPort", "2181");

		Table table = null;
		try {
			Connection connection = ConnectionFactory.createConnection(config);
			// 配置hbase的具体表名
			table = connection.getTable(TableName.valueOf("userInfo"));
			// 设置rowkey的值
			Put put = new Put(Bytes.toBytes("rowkey:1001"));
			// 设置family:qualifier:value
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(15));

			// 使用put类, 写入hbase对应的表中
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
