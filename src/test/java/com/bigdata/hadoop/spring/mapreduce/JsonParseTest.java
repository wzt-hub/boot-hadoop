package com.bigdata.hadoop.spring.mapreduce;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

/**
 * @author wzt 2444722463@qq.com
 * @date 2018/10/18 - 21:16
 */
public class JsonParseTest {
    private ApplicationContext ctx;
    private JobRunner runner;

    @Before
    public void setUp(){
        //初始化ApplicationContext对象
        //设置hadoop运行身份，这样可以绕过系统权限检查
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        ctx= new ClassPathXmlApplicationContext("beans.xml");
        runner= (JobRunner) ctx.getBean("runner");
    }

    @Test
    public void testMR() throws Exception {
        runner.setWaitForCompletion(true);
        runner.call();
    }


    @After
    public void tearDown(){
        //单元测试结束后，将ApplicationContext对象置为空，交给垃圾回收机制回收
        ctx=null;
    }
}
