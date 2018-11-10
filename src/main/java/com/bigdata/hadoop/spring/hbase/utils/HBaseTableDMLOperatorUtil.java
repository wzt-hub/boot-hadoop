package com.bigdata.hadoop.spring.hbase.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * HBase Table 表数据操作
 * @author wzt 2444722463@qq.com
 * @date 2018/10/17 - 9:58
 */
public class HBaseTableDMLOperatorUtil {

    //保存Hbase数据库的连接
    private static Connection conn=null;

    static{
        conn=HBaseConnUtil.getConn();
    }

    /**
     * 删除指定NS下的给定的Table给定row key的指定colName的值
     * @param namespace 名称空间
     * @param tableName 表名
     * @param cfName 列族名
     * @param colName 列名
     * @throws IOException
     */
    public static boolean deleteColumn(String namespace,String tableName,String rowkey,String cfName,String colName) {
        //判断列是否存在
        Result result=getColumn(namespace,tableName,rowkey,cfName,colName);
        if(result.isEmpty()){
            System.out.println("按照指定条件查询的列："+colName+" 不存在！");
            return false;
        }

        try {
            //在判断列是否存在时，已经获取了连接，此时可以直接使用
            Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
            Delete delete=new Delete(rowkey.getBytes());
            if(cfName!=null || cfName!=""){ //cfName为空，Delete对象封装完毕
                if(colName==null || colName==""){
                    delete.addFamily(cfName.getBytes());
                }else{
                    delete.addColumn(cfName.getBytes(),colName.getBytes());
                }
            }
            //执行删除列操作
            table.delete(delete);
            System.out.println("删除表为："+namespace+":"+tableName+"，row key为："+rowkey+"，的列："+colName+" 成功！");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 往指定表的指定rowkey中添加指定cfname和colname的colvalue
     * 一次插入一条数据
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowkey 行键
     * @param cfName 列族名称
     * @param colName 列名
     * @param colValue 列值
     * @throws Exception
     */
    public static boolean putRow(String namespace, String tableName, String rowkey, String cfName, String colName, String colValue) {
        try {
            if(conn==null){
                conn=HBaseConnUtil.getConn();
            }
            //1.根据给定的NS名和表名创建table对象
            Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));

            //2.根据rowkey创建put对象
            Put put = new Put(rowkey.getBytes());

            //3.往指定的rowkey中的列族中插入数据
            put.addColumn(cfName.getBytes(),colName.getBytes(),colValue.getBytes());
            //4.将上述put对象添加到表中 即：将指定rowkey、cfname、colname、colvalue的数据添加到表中
            table.put(put);
            System.out.println("向表："+namespace+":"+tableName+"，row key为："+rowkey+"，插入列："+colName+" 成功！");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * 往指定表插入一组数据
     * @param NSName 命名空间名称
     * @param tableName 表名
     * @param puts 封装一组数据
     * @return
     */
    public static boolean putRows(String NSName, String tableName, List<Put> puts){
        try {
            if(conn==null){
                conn=HBaseConnUtil.getConn();
            }
            //1.根据给定的NS名和表名创建table对象
            Table table = conn.getTable(TableName.valueOf(NSName + ":" + tableName));

            //一次性插入多个数据
            table.put(puts);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 获取一行记录
     * @param namespace
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Result getRow(String namespace, String tableName, String rowKey){
        return getColumn(namespace,tableName,rowKey,null,null);
    }


    /**
     * 获取一列记录
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowKey 行键
     * @param cfName 列族名称
     * @param colName  列名
     * @throws IOException
     */
    public static Result getColumn(String namespace, String tableName, String rowKey, String cfName, String colName){
        try {
            if(conn==null){
                conn=HBaseConnUtil.getConn();
            }
            //1.根据给定的NS名和表名创建table对象
            Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
            //2.根据rowkey构造Get对象
            Get get = new Get(rowKey.getBytes());

            if (cfName!=null && !cfName.equals("")){//查询条件中指定了列族名
                if (colName != null && !cfName.equals("")){//查询条件中同时制定了列族名和列族内的列名
                    get.addColumn(cfName.getBytes(),colName.getBytes());
                }else {//只指定了列族名
                    get.addFamily(cfName.getBytes());
                }
            }
            //执行查询
            Result result = table.get(get);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 获取一行的所有数据
     * @param oneRowResult 一行结果集
     * @throws Exception
     */
    public static void showResult(Result oneRowResult){
        if(!oneRowResult.isEmpty()){
            for(Cell cell : oneRowResult.listCells()){
                System.out.print("行键："+new String(CellUtil.cloneRow(cell))+"\t");
                System.out.print("列族："+new String(CellUtil.cloneFamily(cell))+"\t");
                System.out.print("列："+new String(CellUtil.cloneQualifier(cell))+"\t");
                System.out.print("值："+new String(CellUtil.cloneValue(cell))+"\t");
                System.out.println("时间戳："+cell.getTimestamp());
            }
        }else{
            System.out.println("查询结果为空！");
        }
    }

}
