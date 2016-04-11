package org.dxer.hadoop.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 * 
 * @class FindFriends
 */
public class FindFriends extends Configured implements Tool {

    private static final String RELATION_TYPE_1 = "1-";

    private static final String RELATION_TYPE_2 = "2-";

    private static final String RELATION_SYMBOL = ">";

    /**
     * 
     * mapper任务
     */
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private Text text = new Text();

        private Text nText = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] names = value.toString().split("\t");

            if (names != null && names.length == 2) {
                String name1 = names[0];
                String name2 = names[1];

                if (name1 != null && name2 != null && !name1.endsWith(name2)) {
                    nText.set(name1);
                    text.set(RELATION_TYPE_1 + name1 + ">" + name2);
                    context.write(nText, text);

                    nText.set(name2);
                    text.set(RELATION_TYPE_2 + name1 + ">" + name2);
                    context.write(nText, text);
                }
            }
        }
    }

    /**
     * reduce任务
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                               InterruptedException {
            List<String> knows = new ArrayList<String>();
            List<String> beKnows = new ArrayList<String>();

            // 遍历每个mapper计算出key的数量，并进行求和
            for (Text v: values) {
                String str = v.toString();
                if (str.startsWith(RELATION_TYPE_1)) {
                    str = str.substring(RELATION_TYPE_1.length());
                    String[] names = str.split(RELATION_SYMBOL);

                    if (names != null && names.length == 2) {
                        String name2 = names[1];

                        knows.add(name2);
                    }

                } else if (str.startsWith(RELATION_TYPE_2)) {
                    str = str.substring(RELATION_TYPE_2.length());

                    String[] names = str.split(RELATION_SYMBOL);

                    if (names != null && names.length == 2) {
                        String name1 = names[0];

                        beKnows.add(name1);
                    }
                }
            }

            if (beKnows != null && beKnows.size() > 0 && knows != null && knows.size() > 0) {
                for (int i = 0; i < beKnows.size(); i++) {
                    String name1 = beKnows.get(i);
                    for (int j = 0; j < knows.size(); j++) {
                        String name2 = knows.get(j);

                        context.write(new Text(name1), new Text(name2));
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 配置作业名
        Job job = Job.getInstance(conf, "FindFriends");

        // 设置作业的各个属性
        job.setJarByClass(FindFriends.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true)? 0: 1;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: FindFriends <in> <out>");
            System.exit(2);
        }
        try {
            ToolRunner.run(new FindFriends(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
