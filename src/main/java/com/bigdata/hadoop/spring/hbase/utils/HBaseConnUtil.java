package com.bigdata.hadoop.spring.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;

/**
 * Hbase操作工具类
 * @author wzt 2444722463@qq.com
 * @date 2018/10/16 - 15:33
 */
public class HBaseConnUtil {

    //构造注入HBase配置对象
    private static Configuration conf;
    public HBaseConnUtil(Configuration conf){
        this.conf=conf;
    }

    private static Connection conn=null;
    private static Table table=null;

    private static HBaseAdmin hBaseAdmin=null;

    /**
     * 根据HBase配置信息获取HBase连接对象
     * @return
     */
    public static Connection getConn(){
        //如果连接对象为空，或者连接已经关闭
        if(conn==null || conn.isClosed()){
            //创建连接对象
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                throw new RuntimeException("创建连接失败！"+e.getMessage());
            }
        }

        return conn;
    }


    /**
     * 关闭HBase 连接对象
     */
    public static void closeConn(){
        if(conn!=null){
            try {
                conn.close();
                System.out.println("连接关闭成功");
            } catch (IOException e) {
                throw new RuntimeException("关闭连接失败！"+e.getMessage());
            }finally {
                conn=null;
            }
        }
    }

    /**
     * 从连接对象中取Table对象
     * @return Table
     */
    public static Table getTable(TableName tableName){
        try {
            if(table==null){
                table=getConn().getTable(tableName);
                System.out.println("获取Table对象成功！");
            }
        } catch (IOException e) {
            throw new RuntimeException("获取Table对象失败！"+e.getMessage());
        }
        return table;
    }

    /**
     * 从连接对象中取HBaseAdmin对象
     * @return
     */
    public static HBaseAdmin getHBaseAdmin(){
        try {
            if(hBaseAdmin==null){
                hBaseAdmin=(HBaseAdmin) getConn().getAdmin();
                System.out.println("获取HBaseAdmin对象成功！");
            }
        } catch (IOException e) {
            throw new RuntimeException("获取HBaseAdmin对象失败！"+e.getMessage());
        }
        return hBaseAdmin;
    }

    /**
     * 关闭Table对象
     * @return
     */
    public static void closeTable(){
        try {
            if(table!=null){
                table.close();
                System.out.println("关闭table对象成功！");
            }
        } catch (IOException e) {
            throw new RuntimeException("关闭table对象失败！"+e.getMessage());
        }finally {
            table=null;
        }
    }
}
