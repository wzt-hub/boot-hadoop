package com.bigdata.hadoop.spring.hbase;

import com.bigdata.hadoop.spring.hbase.utils.HBaseConnUtil;
import com.bigdata.hadoop.spring.hbase.utils.HBaseNSOperatorUtil;
import com.bigdata.hadoop.spring.hbase.utils.HBaseTableDDLOperatorUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author wzt 2444722463@qq.com
 * @date 2018/11/9 - 21:54
 */
public class HBaseTableDDLOperatorUtilTest {
    private ApplicationContext ctx;


    @Before
    public void setUp(){
        //初始化ApplicationContext对象
        ctx= new ClassPathXmlApplicationContext("beans.xml");
    }

    /**
     * 测试创建 HBase 的表结构
     */
    @Test
    public void createTable(){
        boolean result = HBaseTableDDLOperatorUtil.createTable("wzt", "test", "info");
        System.out.println(result);
    }

    /**
     * 测试删除 HBase 的表结构
     */
    @Test
    public void delTable(){
        boolean result = HBaseTableDDLOperatorUtil.delTable("wzt", "test");
        System.out.println(result);
    }

    /**
     * 测试遍历当前命名空间下所有的table信息
     */
    @Test
    public void listNamespaceTable(){
        HBaseTableDDLOperatorUtil.listNamespaceTable("wzt");
    }



    @After
    public void tearDown(){
        //单元测试结束后，将ApplicationContext对象置为空，交给垃圾回收机制回收
        ctx=null;
    }
}
