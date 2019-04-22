package day01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfs {
    //
    FileSystem fileSystem=null;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
       Configuration conf = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
   }
   //上传文件到hdfs
   @Test
   public void upload() throws IOException {

       fileSystem.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\java大数据笔记\\LInux笔记\\hadoop\\hadoop介绍.txt"),new Path("/user"));


   }
  //下载文件
    @Test
    public void download(){
        try {
            fileSystem.copyToLocalFile(false,new Path("/user/hadoop介绍.txt"),new Path("E:\\"),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //创建
    @Test
    public void create(){
        try {
            boolean mkdirs = fileSystem.mkdirs(new Path("/baidu.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //删除
    @Test
    public void remove(){
        try {
            fileSystem.delete(new Path("/b.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //查看
    @Test
    public void cat() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/user"), true);
         while (iterator.hasNext()){
             LocatedFileStatus fileStatus = iterator.next();
             System.out.println(fileStatus.getPath()+""+fileStatus.getLen());
         }
    }
   @After
    public void close(){
       try {
           fileSystem.close();
       } catch (IOException e) {
           e.printStackTrace();
       }
   }
}
