package day03;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class MyFriends2 {
    static class MapTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A	I,K,C,B,G,F,H,O,D
            String[] split = value.toString().split("\t");
            String val = split[0];
            String[] values = split[1].split(",");
            Arrays.sort(values);
            for (int i=0;i<values.length-1;i++){
                for (int j=i+1;j<values.length;j++){
                    String line = values[i]+"-"+values[j];
                    context.write(new Text(line),new Text(val));
                }
            }
        }
    }
    static class ReudceTask extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag){
                    sb.append(value);
                    flag=false;
                }else {
                    sb.append(",").append(value);
                }
            }
            context.write(new Text(key),new Text(sb.toString()));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MyFriends2.MapTask.class);
        job.setReducerClass(MyFriends2.ReudceTask.class);
        job.setJarByClass(MyFriends2.class);
        //设置输出信息
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出信息
        File file = new File("E:\\test\\test2");
        if (file.exists()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("E:\\movie\\"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\test\\test2"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"老铁没毛病":"神仙bug");

    }
}
