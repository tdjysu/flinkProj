package com.df;

import com.alibaba.fastjson.JSONObject;
import com.df.DimSource.OrgaRedisSourceJava;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * 数据清洗
 * 创建主题allData bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic allData
 *
 */

public class IntentCleanJava {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        指定kafka Source
        String intopic = "intent_t1";
        String outTopic = "intent_t2";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.207:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        prop.setProperty("group.id", "con1");
//      设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");


        //      checkpoint配置
        env.enableCheckpointing(60000);//每60秒检查一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        FlinkKafkaConsumer010 myConsumer =  new FlinkKafkaConsumer010<String>(intopic,new SimpleStringSchema(),prop);

//      获取kafka中的数据
        DataStream<String> data = env.addSource(myConsumer);
//      从Redis中获取维度数据
        DataStream<HashMap<String, String[]>> dimData = env.addSource( new OrgaRedisSourceJava()).broadcast();

//        DataStream<String> resData = data.connect(dimData).flatMap(new ControlFunction());

        data.connect(dimData).flatMap(new ControlFunction()).print();


//        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);


//        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),prop);
//        resData.addSink(myProducer);
        env.execute(IntentCleanJava.class.getName());
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String,HashMap<String,String[]>,String>{
        // 营业部维度关系
        HashMap<String,String[]>  orgDimMap = new HashMap<String,String[]>();


        @Override
        public void flatMap1(String value, Collector<String> collector) throws Exception {
            JSONObject jsonObject = JSONObject.parseObject(value);
            String deptcode = jsonObject.getString("strdeptcode");

//          通过营业部编码获取其它组织机构信息
            String[] orgArray = orgDimMap.get(deptcode);
            String detpname = orgArray[0];
            String busiAreaCode = orgArray[1];
            String busiAreaName = orgArray[2];
            String adminAreaCode = orgArray[3];
            String adminAreaName = orgArray[4];
            jsonObject.put("detpname",detpname);
            jsonObject.put("busiAreaCode",busiAreaCode);
            jsonObject.put("busiAreaName",busiAreaName);
            jsonObject.put("adminAreaCode",adminAreaCode);
            jsonObject.put("adminAreaName",adminAreaName);
            collector.collect(jsonObject.toJSONString());
System.out.println(jsonObject.toJSONString());

        }

        @Override
        public void flatMap2(HashMap<String, String[]> value2, Collector<String> collector) throws Exception {
            this.orgDimMap = value2;

        }
    }
}
