package com.bigdata.hadoop.spring.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * 使用Spring Hadoop来操作HDFS文件系统
 * @author wzt 2444722463@qq.com
 * @date 2018/10/18 - 17:42
 */
public class HDFSAPITest {
    private ApplicationContext ctx;
    private FileSystem fileSystem;


    @Before
    public void setUp(){
        //初始化ApplicationContext对象
        ctx= new ClassPathXmlApplicationContext("beans.xml");
        fileSystem= (FileSystem) ctx.getBean("fileSystem");
    }

    /**
     * 在HDFS上创建文件夹
     */
    @Test
    public void testMkdir() {
        try {

            boolean result = fileSystem.mkdirs(new Path("/spring/hdfs2"));
            if(result){
                System.out.println("创建文件夹成功！");
            }else{
                System.out.println("创建文件夹失败！");
            }
        } catch (IOException e) {
            throw new RuntimeException("创建文件夹失败！");
        }
    }


    @After
    public void tearDown(){
        //单元测试结束后，将ApplicationContext对象置为空，交给垃圾回收机制回收
        ctx=null;
    }
}
