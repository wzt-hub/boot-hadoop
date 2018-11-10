package com.bigdata.hadoop.spring.hbase.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase 表结构操作
 * @author wzt 2444722463@qq.com
 * @date 2018/10/16 - 18:42
 */
public class HBaseTableDDLOperatorUtil{

    //保存HBase数据库的连接
    private static Connection conn=null;
    static{
        conn=HBaseConnUtil.getConn();
    }

    /**
     * 创建一个HBase 的表结构
     * @param namespace 表所属的名称空间
     * @param tableName 表的名称
     * @param FamilyColumns 列族
     */
    public static boolean createTable(String namespace,String tableName,String ...FamilyColumns) {
        if(conn==null){
            conn=HBaseConnUtil.getConn();
        }
        //第一步：首先判断名称空间是否存在,若不存在则创建名称空间
        boolean isExists = HBaseNSOperatorUtil.isExistsNamespace(conn,namespace);
        if(!isExists){ //如果名称空间不存在，则创建名称空间
            HBaseNSOperatorUtil.createNamespace(conn,namespace);
        }

        //判断表是否已经存在
        if(isExistTable(conn,namespace,tableName)){
            System.out.println("命名空间："+namespace+" 已存在表："+tableName+" ！");
            return false;
        }

        try {
            //第二步：创建表
            HBaseAdmin hbaseAdmin = (HBaseAdmin)conn.getAdmin();
            //构建一个表的描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace + ":" + tableName));
            //存储列族
            List<ColumnFamilyDescriptor> cfs=new ArrayList<>();
            //遍历列族
            for(String familyColumn:FamilyColumns){
                //封装传入的列族字符串
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyColumn)).build();
                cfs.add(columnFamilyDescriptor);
            }
            //封装列族对象到表描述器
            TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamilies(cfs).build();
            //创建表
            hbaseAdmin.createTable(tableDescriptor);
            System.out.println("创建命名空间："+namespace+" 的表："+tableName+" 成功！");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * 遍历当前命名空间下所有的table信息
     * @param namespace 当前命名空间的名称
     */
    public static void listNamespaceTable(String namespace){
        if(conn==null){
            conn=HBaseConnUtil.getConn();
        }
        listNamespaceTable(conn,namespace);
    }

    /**
     * 遍历当前命名空间下所有的table信息
     * @param conn 连接
     * @param namespace 当前命名空间的名称
     */
    public static void listNamespaceTable(Connection conn,String namespace) {
        //判断名称空间是否存在
        boolean isExists = HBaseNSOperatorUtil.isExistsNamespace(conn,namespace);
        if(!isExists){
            System.out.println("命名空间："+namespace+" 不存在！");
            return;
        }
        try {
            HBaseAdmin hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //获取当前命名空间下的所有表
            TableName[] tableNames = hbaseAdmin.listTableNamesByNamespace(namespace);
            System.out.println("命名空间："+namespace);
            if(tableNames.length==0){
                System.out.println("当前命名空间下，暂无表信息！");
            }
            for(TableName tableName:tableNames){
                System.out.println("表名："+tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断指定命名空间中指定的表是否存在
     * @param tableName
     * @return
     */
    public static boolean isExistTable(Connection conn,String namespace,String tableName) {
        boolean flag=false;
        //拼接表名
        String nsTName=namespace+":"+tableName;
        try {
            HBaseAdmin hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //获取当前命名空间下所有的表名称
            TableName[] tableNames = hbaseAdmin.listTableNamesByNamespace(namespace);
            for(TableName tName:tableNames){
                //去掉命名空间，截取表名
                if(nsTName.equals(tName.getNameAsString())){
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
     * 删除指定命名空间下的表
     * @param namespace 命名空间的名称
     * @param tableName 表名
     */
    public static boolean delTable(String namespace,String tableName) {
        if(conn==null){
            conn=HBaseConnUtil.getConn();
        }
        //判断名称空间是否存在
        if(!HBaseNSOperatorUtil.isExistsNamespace(conn,namespace)){
            System.out.println("指定的命名空间："+namespace+" 不存在！");
            return false;
        }

        //判断当前命名空间下是否存在该表
        if(!isExistTable(conn,namespace,tableName)){
            System.out.println("命名空间："+namespace+" 下不存在表："+tableName);
            return false;
        }
        HBaseAdmin hbaseAdmin = null;

        //消除重复禁用表的异常
        try {
            hbaseAdmin = (HBaseAdmin) conn.getAdmin();
            //禁用Table
            hbaseAdmin.disableTable(TableName.valueOf(namespace+":"+tableName));
        } catch (IOException e) {
        }

        //删除表
        try {
            hbaseAdmin.deleteTable(TableName.valueOf(namespace+":"+tableName));
            System.out.println("删除表：" + namespace + ":" + tableName + "成功！");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
