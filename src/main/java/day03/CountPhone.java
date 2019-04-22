package day03;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
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

public class CountPhone {
    static class MapTask extends Mapper<LongWritable, Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (split.length==7) {
                    StringBuffer sb = new StringBuffer();
                    sb.append(split[0].substring(0,3));
                    sb.append(split[1]);
                    context.write(new Text(sb.toString()+"\t"),new Text(split[3]));
            }
        }
    }
    static class ReduceTask extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
                break;
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置输出类型
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(CountPhone.class);
        //设置输出信息
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出信息
        File file = new File("E:\\phone");
        if (file.exists()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("E:\\phone.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\phone"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"你真棒":"报错了");
    }
}
