package org.dxer.hadoop.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @class WordCount
 */
public class WordCount extends Configured implements Tool {

    /**
     * 
     * mapper任务
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable count = new IntWritable(1);

        private Text text = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreElements()) {
                text.set(tokenizer.nextToken());
                context.write(text, count);
            }
        }
    }

    /**
     * reduce任务
     */
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // 记录word的总次数
        private IntWritable sum = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                                                                                      InterruptedException {
            int count = 0;
            // 遍历每个mapper计算出key的数量，并进行求和
            for (IntWritable v: values) {
                count += v.get();
            }
            sum.set(count);

            // 记录结果
            context.write(key, sum);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 配置作业名
        Job job = Job.getInstance(conf, "wordCount");

        // 设置作业的各个属性
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true)? 0: 1;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }
        try {
            ToolRunner.run(new WordCount(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
