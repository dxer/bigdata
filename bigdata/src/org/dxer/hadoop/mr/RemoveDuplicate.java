package org.dxer.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 去重
 * 
 * @class RemoveDuplicate
 */
public class RemoveDuplicate extends Configured implements Tool {

    /**
     * 直接将value作为map任务的输出 mapper任务
     */
    public static class RemoveDuplicateMapper extends Mapper<Object, Text, Text, Text> {

        private Text text = new Text();

        private static Text nullText = new Text("");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            text.set(value.toString());
            context.write(text, nullText);
        }
    }

    /**
     * 将reduce任务的输入直接输出即可 reduce任务
     */
    public static class RemoveDuplicateReducer extends Reducer<Text, Text, Text, Text> {

        private static Text nullText = new Text("");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                               InterruptedException {
            context.write(key, nullText);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 配置作业名
        Job job = Job.getInstance(conf, "removeDuplicate");

        // 设置作业的各个属性
        job.setJarByClass(RemoveDuplicate.class);
        job.setMapperClass(RemoveDuplicateMapper.class);
        job.setCombinerClass(RemoveDuplicateReducer.class);
        job.setReducerClass(RemoveDuplicateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true)? 0: 1;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: RemoveDuplicate <in> <out>");
            System.exit(2);
        }
        try {
            ToolRunner.run(new WordCount(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
