package com.bigdata.hadoop.spring.hbase.utils;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


/**
 * HBase 命名空间操作API
 * @author wzt 2444722463@qq.com
 * @date 2018/10/16 - 17:03
 */
public class HBaseNSOperatorUtil {
    //保存HBase数据库的连接
    private static Connection conn=null;
    static{
        conn=HBaseConnUtil.getConn();
    }

    /**
     * 根据命名空间名称创建命名空间Namespace
     * @param namespace
     * @return
     */
    public static boolean createNamespace(String namespace){
        if(conn==null){
            conn=HBaseConnUtil.getConn();
        }
        return createNamespace(conn,namespace);
    }

    /**
     * 根据HBase连接和命名空间名称创建命名空间Namespace
     * @param conn HBase连接
     * @param namespace 命名空间
     * @return
     */
    public static boolean createNamespace(Connection conn,String namespace){
        //判断名称空间是否存在
        if(isExistsNamespace(conn,namespace)){
            System.out.println("创建失败，命名空间："+namespace+" 已经存在！");
            return false;
        }

        try {
            //1.获取HBaseAdmin对象
            HBaseAdmin hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //2.创建命名空间描述对象
            NamespaceDescriptor descriptor=NamespaceDescriptor.create(namespace).build();
            //3.创建命名空间
            hbaseAdmin.createNamespace(descriptor);
            System.out.println("命名空间："+namespace+" 创建成功！");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 判断名称空间是否存在
     * @param namespace
     * @return
     */
    public static boolean isExistsNamespace(String namespace){
        if(conn==null){
            conn=HBaseConnUtil.getConn();
        }
        return isExistsNamespace(conn,namespace);
    }

    /**
     * 根据HBase连接和名称空间的名称判断名称空间是否存在
     * @param namespace 名称空间的名称
     */
    public static boolean isExistsNamespace(Connection conn, String namespace){

        //标识名称空间是否存在
        boolean flag=false;
        try {
            //1.获取HBaseAdmin对象
            HBaseAdmin hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //2.使用HbaseAdmin对象获取所有的命名空间描述信息
            NamespaceDescriptor[] namespaceDescriptors = hbaseAdmin.listNamespaceDescriptors();
            //遍历整个集群的名称空间
            for(NamespaceDescriptor descriptor:namespaceDescriptors){
                if(namespace.equals(descriptor.getName())){
                    flag=true;
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 查看当前Hbase集群中的所有的命名空间
     */
    public static void listNamespaces(){
        try {
            if(conn==null){
                conn=HBaseConnUtil.getConn();
            }
            //1.获取HBaseAdmin对象
            HBaseAdmin hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //2.使用HbaseAdmin对象获取所有的命名空间描述信息
            NamespaceDescriptor[] namespaceDescriptors = hbaseAdmin.listNamespaceDescriptors();

            //3.循环遍历每个命名空间，打印命名空间的名称
            for(NamespaceDescriptor descriptor:namespaceDescriptors){
                System.out.println(descriptor.getName());
            }
            //4.统计所有的命名空间的总个数
            System.out.println(namespaceDescriptors.length+" row(s)");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一个空的命名空间
     * @param namespace 命名空间名称
     */
    public static boolean delEmptyNamespace(String namespace){
        //判断名称空间是否存在
        if(!isExistsNamespace(namespace)){
            System.out.println("删除失败，命名空间："+namespace+" 不存在！");
            return false;
        }
        try {
            if(conn==null){
                conn=HBaseConnUtil.getConn();
            }
            //1.获取HBaseAdmin对象
            HBaseAdmin hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //2.直接删除命名空间
            hbaseAdmin.deleteNamespace(namespace);
            System.out.println("空命名空间："+namespace+" 删除成功！");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 级联删除非空命名空间
     * @param namespace
     */
    public static boolean delNotEmptyNamespace(String namespace) {
        //判断名称空间是否存在
        if(!isExistsNamespace(namespace)){
            System.out.println("删除失败，命名空间："+namespace+" 不存在！");
            return false;
        }
        try {
            if(conn==null){
                conn=HBaseConnUtil.getConn();
            }
            //1.根据HBaseDriverManager获取HBaseAdmin对象
            HBaseAdmin hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //2.获取当前Namespace下所有的表
            TableName[] tableNames = hbaseAdmin.listTableNamesByNamespace(namespace);

            for(TableName tableName:tableNames){
                //禁用Table
                hbaseAdmin.disableTable(tableName);
                //删除Table
                hbaseAdmin.deleteTable(tableName);
                System.out.println("删除表："+tableName.getNameAsString()+" 删除成功！");
            }

            //删除空命名空间
            delEmptyNamespace(namespace);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 删除指定的Namespace
     * @param namespaceName 要删除命名空间的名称
     * @param isEmpty 要删除命名空间是否为空
     */
    public static boolean delNamespace(String namespaceName,boolean isEmpty){
        if(isEmpty){
            return delEmptyNamespace(namespaceName);
        }else{
            return delNotEmptyNamespace(namespaceName);
        }
    }

}
