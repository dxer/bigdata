package org.dxer.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Bytes.RowEndKeyComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImportTask extends Configured implements Tool {

	public static byte[] ACCESS_CF = Bytes.toBytes("info");

	public static byte[] C_ACCESSTIME = Bytes.toBytes("accessTime");
	public static byte[] C_ACCOUNT = Bytes.toBytes("account");
	public static byte[] C_IP = Bytes.toBytes("ip");
	public static byte[] C_HOST = Bytes.toBytes("host");
	public static byte[] C_APPKEY = Bytes.toBytes("appkey");
	public static byte[] C_RESULT = Bytes.toBytes("result");
	public static byte[] C_APINAME = Bytes.toBytes("apiName");
	public static byte[] C_COST = Bytes.toBytes("cost");
	public static byte[] C_REQDATA = Bytes.toBytes("reqData");
	public static byte[] C_RESPDTA = Bytes.toBytes("respData");

	public Put mkPut(String line) {
		Put put = null;

		if (line == null || line.length() <= 0) {
			return put;
		}

		String[] strs = line.split("\t");
		if (strs != null && strs.length == 16) {
			String accesstime = strs[0];
			String account = strs[1];
			String ip = strs[2];
			String host = strs[3];
			String appkey = strs[3];
			String result = strs[4];
			String cost = strs[5];
			String apiName = strs[6];
			String reqData = strs[7];
			String respData = strs[8];

			// rowkey => account+"<>"+(Long.Max_Value - timestamp)
			String rowKey = account + "<>" + (Long.MAX_VALUE - Long.parseLong(accesstime)); // 组装rowkey

			put = new Put(Bytes.toBytes(rowKey));

			put.addColumn(ACCESS_CF, C_ACCESSTIME, Bytes.toBytes(accesstime));
			put.addColumn(ACCESS_CF, C_IP, Bytes.toBytes(ip));
			put.addColumn(ACCESS_CF, C_HOST, Bytes.toBytes(host));
			put.addColumn(ACCESS_CF, C_COST, Bytes.toBytes(cost));
			put.addColumn(ACCESS_CF, C_APPKEY, Bytes.toBytes(appkey));
			put.addColumn(ACCESS_CF, C_RESULT, Bytes.toBytes(result));
			put.addColumn(ACCESS_CF, C_APINAME, Bytes.toBytes(apiName));
			put.addColumn(ACCESS_CF, C_RESPDTA, Bytes.toBytes(respData));
			put.addColumn(ACCESS_CF, C_REQDATA, Bytes.toBytes(reqData));
		}

		return put;
	}

	class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		@Override
		protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {

			Put put = mkPut(value.toString());

			if (put != null) {
				ImmutableBytesWritable bw = new ImmutableBytesWritable();

				context.write(bw, put);
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Job job = Job.getInstance(conf);

		job.setJobName("importTask");
		job.setMapperClass(ImportMapper.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setJarByClass(getClass());

		// 只需要mapper即可，设置reduce的任务为0
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new ImportTask(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
