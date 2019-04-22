package day03;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class MyFrends {
    static class MapTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            String user = split[0];
            String[] frends = split[1].split(",");
            for (String list : frends) {
               context.write(new Text(list),new Text(user));
            }

        }
    }
    static class ReduceTask extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag){
                    sb.append(value);
                    flag = false;
                }else {
                    sb.append(",").append(value);
                }

            }
            context.write(key,new Text(sb.toString()));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MapTask.class);
        job.setReducerClass(MyFrends.ReduceTask.class);
        job.setJarByClass(MyFrends.class);
        //设置输出信息
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出信息
        File file = new File("E:\\movie\\");
        if (file.exists()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("E:\\friend.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\movie\\"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"老铁没毛病":"神仙bug");

    }
}
