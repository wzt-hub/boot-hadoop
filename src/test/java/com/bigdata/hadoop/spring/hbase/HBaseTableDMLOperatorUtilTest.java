package com.bigdata.hadoop.spring.hbase;

import com.bigdata.hadoop.spring.hbase.utils.HBaseTableDMLOperatorUtil;
import org.apache.hadoop.hbase.client.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author wzt 2444722463@qq.com
 * @date 2018/11/10 - 11:15
 */
public class HBaseTableDMLOperatorUtilTest {

    private ApplicationContext ctx;


    @Before
    public void setUp(){
        //初始化ApplicationContext对象
        ctx= new ClassPathXmlApplicationContext("beans.xml");
    }


    /**
     * 往指定表的指定rowkey中添加指定cfname和colname的colvalue
     */
    @Test
    public void put(){
        HBaseTableDMLOperatorUtil.putRow("wzt","test","1001","info","id","1");
        HBaseTableDMLOperatorUtil.putRow("wzt","test","1001","info","name","zs");
        HBaseTableDMLOperatorUtil.putRow("wzt","test","1001","info","age","15");
        HBaseTableDMLOperatorUtil.putRow("wzt","test","1001","info","sex","male");

        HBaseTableDMLOperatorUtil.putRow("wzt","test","1002","info","id","100");
        HBaseTableDMLOperatorUtil.putRow("wzt","test","1002","info","name","lisi");
        HBaseTableDMLOperatorUtil.putRow("wzt","test","1002","info","age","35");
    }


    /**
     * 在给定的NS下的指定的表中按照给定的查询条件查询数据
     */
    @Test
    public void getColumn(){
        Result res=HBaseTableDMLOperatorUtil.getColumn("wzt","test","1001","info","name");
        if(!res.isEmpty()){
            //打印结果
            HBaseTableDMLOperatorUtil.showResult(res);
        }
    }


    /**
     * 获取一行内容
     */
    @Test
    public void getRow(){
        Result res=HBaseTableDMLOperatorUtil.getRow("wzt","test","1001");
        if(!res.isEmpty()){
            //打印结果
            HBaseTableDMLOperatorUtil.showResult(res);
        }
    }


    /**
     * 在给定的NS下的指定的表中按照给定的查询条件查询数据
     */
    @Test
    public void deleteColumn(){
        boolean result = HBaseTableDMLOperatorUtil.deleteColumn("wzt",
                "test", "1001", "info", "id");
        System.out.println(result);
    }


    @After
    public void tearDown(){
        //单元测试结束后，将ApplicationContext对象置为空，交给垃圾回收机制回收
        ctx=null;
    }
}
