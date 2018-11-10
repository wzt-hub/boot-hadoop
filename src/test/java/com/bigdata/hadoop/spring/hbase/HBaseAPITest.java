package com.bigdata.hadoop.spring.hbase;

import com.bigdata.hadoop.spring.hbase.utils.HBaseConnUtil;
import com.bigdata.hadoop.spring.hbase.utils.HBaseNSOperatorUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * 使用Spring Hadoop来操作HBase数据库
 * @author wzt 2444722463@qq.com
 * @date 2018/10/17 - 20:51
 */
public class HBaseAPITest {
    private ApplicationContext ctx;


    @Before
    public void setUp(){
        //初始化ApplicationContext对象
        ctx= new ClassPathXmlApplicationContext("beans.xml");
    }

    /**
     * 测试管理HBase的连接
     */
    @Test
    public void getHBaseConn(){
        //获取HBase连接
        Connection conn = HBaseConnUtil.getConn();
        System.out.println(conn);
        //关闭连接
        HBaseConnUtil.closeConn();
    }

    /**
     * 从连接对象中获取Table对象
     */
    @Test
    public void getTable(){
        //根据表名获取Table对象
        Table table = HBaseConnUtil.getTable(TableName.valueOf("ns:emp1"));
        System.out.println(table.getName().getNameAsString());

        //关闭
        HBaseConnUtil.closeTable();
    }

    /**
     * 从连接对象中获取HbaseAdmin对象
     */
    @Test
    public void getHBaseAdmin(){
        //根据表名获取Table对象
        HBaseAdmin admin = HBaseConnUtil.getHBaseAdmin();
        System.out.println(admin);

        //关闭
        HBaseConnUtil.closeTable();
    }

    /**
     * 判断命名空间是否存在
     */
    @Test
    public void isExistsNamespace(){
        boolean result = HBaseNSOperatorUtil.isExistsNamespace("wzt2");
        System.out.println(result);
    }


    /**
     * 测试创建命名空间
     */
    @Test
    public void createNamespace(){
        boolean result = HBaseNSOperatorUtil.createNamespace("wzt");
        System.out.println(result);
    }

    /**
     * 查看当前Hbase集群中的所有的命名空间
     */
    @Test
    public void listNamespaces(){
        HBaseNSOperatorUtil.listNamespaces();
    }

    /**
     * 删除一个空的命名空间
     */
    @Test
    public void delEmptyNamespace(){
        boolean result = HBaseNSOperatorUtil.delEmptyNamespace("wzt");
        System.out.println(result);
    }

    /**
     * 级联删除非空命名空间
     */
    @Test
    public void delNotEmptyNamespace(){
        boolean result = HBaseNSOperatorUtil.delNotEmptyNamespace("wzt");
        System.out.println(result);
    }

    /**
     * 根据Namespace是否为空进行删除
     */
    @Test
    public void delNamespace(){
        HBaseNSOperatorUtil.delNamespace("wzt2",false);
    }

    @After
    public void tearDown(){
        //单元测试结束后，将ApplicationContext对象置为空，交给垃圾回收机制回收
        ctx=null;
    }

}
