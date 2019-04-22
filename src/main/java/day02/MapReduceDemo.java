package day02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceDemo {
    //前两个是输入端固定类型，LongWritable是下标后两个是输出端Text相当于String类型,IntWritable相当于int类型
     static class MapTask extends Mapper<LongWritable, Text,Text, IntWritable>{
        //map端读取数据并处理
         @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            for (String word : splits) {
                context.write(new Text(word),new IntWritable(1));
            }

        }

    }
    static class ReduceTask extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable words : values) {
                count++;
            }
            System.out.println(key);
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(MapReduceDemo.class);
        //设置输出信息
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入输出信息
        FileInputFormat .addInputPath(job,new Path("E:\\index.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\test"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"老铁没毛病":"神仙bug");

    }
}
