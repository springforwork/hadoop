package day03;

import day02.MapReduceDemo;
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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class MoveAVG {
    static class MapTaker extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            ObjectMapper objectMapper = new ObjectMapper();
            Move move = null;
             move = objectMapper.readValue(value.toString(), Move.class);
             context.write(new Text(move.getMovie()),new IntWritable(move.getRate()));
        }
    }
    static class ReduceTask extends Reducer<Text,IntWritable,Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum  = 0;
            for (IntWritable value : values) {
                int i = value.get();
                sum +=i;
                count++;
            }
            double avg = sum/count;
            context.write(key,new DoubleWritable(avg));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MapTaker.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(MoveAVG.class);
        //设置输出信息
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //设置输入输出信息
        File file = new File("E:\\movie");
        if (file.exists()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("E:\\作业\\movie.json"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\movie"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"老铁没毛病":"神仙bug");

    }

}
