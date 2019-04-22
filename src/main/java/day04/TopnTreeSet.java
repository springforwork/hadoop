package day04;

import day03.Move;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

//每部电影的前5个解决内存溢出
public class TopnTreeSet {
    static class MapTask extends Mapper<LongWritable, Text,Text, Move>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            ObjectMapper mapper = new ObjectMapper();
            Move movie = mapper.readValue(value.toString(), Move.class);
            context.write(new Text(movie.getMovie()),movie);
        }
    }
    static class ReduceTask extends Reducer<Text,Move,Move, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Move> values, Context context) throws IOException, InterruptedException {
            TreeSet<Move> moves = new TreeSet<>(new Comparator<Move>() {
                @Override
                public int compare(Move o1, Move o2) {
                    return o2.getRate()-o1.getRate();
                }
            });
            for (Move move : values) {
                if (moves.size()<5){
                    moves.add(move);
                }else {
                    Move last = moves.last();
                    if (last.getRate()<move.getRate()){
                        moves.remove(last);
                        moves.add(move);
                    }
                }
            }
            for (Move move : moves) {
                context.write(move,NullWritable.get());
            }
            //求前5名

        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(TopnTreeSet.MapTask.class);
        job.setReducerClass(TopnTreeSet.ReduceTask.class);
        job.setJarByClass(TopnTreeSet.class);
        //设置输出信息
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Move.class);
        job.setOutputKeyClass(Move.class);
        job.setOutputValueClass(NullWritable.class);
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
