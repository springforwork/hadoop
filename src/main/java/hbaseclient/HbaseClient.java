package hbaseclient;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseClient {
    public static void main(String[] args) {

    }
    public static Connection connection;
    static {
        //获取hbase连接
        HBaseConfiguration conf = new HBaseConfiguration();
        //设置参数
        conf.set("hbase.zookeeper.quorum","192.168.88.100");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取连接对象
        try {
             connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //判断表是否存在
    public static boolean exits(String table) throws Exception {
       Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(table));
        return b;
    }
    public static void createtable(String table,String...cf){

    }
}
