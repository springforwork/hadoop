package day01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfsIo {
    FileSystem fileSystem=null;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
    }
    @Test
    public void read() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/user/aaa.txt"));
        FSDataOutputStream create = fileSystem.create(new Path("/b.txt"));
        //读取数据
        BufferedReader br = new BufferedReader(new InputStreamReader(open));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(create));
        String line=null;
        while((line=br.readLine())!=null){
            bw.write(line);
            bw.newLine();
            bw.flush();
        }
        br.close();
        bw.close();
    }
}
