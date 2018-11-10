package com.bigdata.hadoop.spring.mapreduce.map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Set;

/**
 * 将json解析为普通文本类型
 */
public class JsonParseMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Text k=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        StringBuilder builder=new StringBuilder();
        for(int i=0;i<16;i++){
            builder.append(fields[i]+"\t");
        }

        //解析json
        String jsonStr=fields[16];
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        //遍历所有的key值
        //考试号
        Set<String> strs = jsonObject.keySet();
        if(strs.size()>0&&strs!=null){
            for(String str:strs){
                JSONObject jo = (JSONObject) jsonObject.get(str);
                //获取某道题考试成绩
                Integer score = jo.getInteger("score");
                builder.append(str+"_"+score+",");
            }
            //去掉末尾的","
            builder.setLength(builder.length()-1);

        }
        builder.append("\t");

        for(int k=17;k<fields.length-1;k++){
            builder.append(fields[k]+"\t");
        }
        //去掉末尾的"\t"
        builder.setLength(builder.length()-1);

        context.write(new Text(builder.toString()),NullWritable.get());
    }

}
