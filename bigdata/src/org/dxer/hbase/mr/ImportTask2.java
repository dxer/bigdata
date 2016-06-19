package org.dxer.hbase.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorImpl;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImportTask2 extends Configured implements Tool {

	private static final int BATCH_SIZE = 200;

	class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		private List<Put> puts = new ArrayList<Put>();

		private HTable table;

		@Override
		protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			table = null;

			table.setWriteBufferSize(1024 * 1024 * 4);

		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {

			Put put = null;

			puts.add(put);

			if (puts != null && puts.size() >= BATCH_SIZE) {
				table.put(puts);
				puts.clear();
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			if (puts != null && puts.size() >= BATCH_SIZE) {
				table.put(puts);
				puts.clear();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		/*
		 * 
		 * 可以设置一些自定义属性
		 */

		Job job = Job.getInstance(conf);

		job.setJobName("inportTask");
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
			ToolRunner.run(new ImportTask2(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
