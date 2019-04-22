package day02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

public class Sing {
    public static void main(String[] args) throws IOException {
        //默认为读取本地文件
        Configuration conf = new Configuration();
        //设置读取HDFS文件
        //conf.set("fs.defaultFS","hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        //读取数据
        FSDataInputStream open = fileSystem.open(new Path("E:\\index.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(open));
        //map处理数据
        Map<String,Integer> map = new HashMap<>();
        String line = null;
        while((line=br.readLine())!=null){
            String[] split = line.split(" ");
            for (String word : split) {
                Integer count = map.getOrDefault(word, 0);
                count++;
                map.put(word,count);
            }
        }

        //reduce
        //写出路径
        FSDataOutputStream out = fileSystem.create(new Path("E:\\作业\\test.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        //遍历map
        Set<Map.Entry<String, Integer>> entrys = map.entrySet();
        List<Map.Entry<String, Integer>> list = new ArrayList<>(entrys);
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
               // return o1.getValue()-o2.getValue();
                return o2.getKey().compareTo(o1.getKey());
            }
        });
        for (Map.Entry<String, Integer> entry : list) {
              bw.write(entry.getKey()+"="+entry.getValue()+"\t\n");
              bw.newLine();
        }
        //关流
        br.close();
        bw.close();

    }
}
