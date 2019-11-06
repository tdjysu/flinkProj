package com.df;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.df.DimSource.DimSource4Redis;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * 数据清洗
 * 创建主题allData bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic allData
 *
 */

public class DataCleanJava {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        指定kafka Source
        String topic = "allData";
        String brokerList = "localhost:9092";
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


        FlinkKafkaConsumer010 myConsumer =  new FlinkKafkaConsumer010<String>(topic,new SimpleStringSchema(),prop);

//      获取kafka中的数据
        DataStreamSource data = env.addSource(myConsumer);
//      从Redis中获取维度数据
        DataStream<HashMap<String, String>> dimData = env.addSource( new DimSource4Redis()).broadcast();

        DataStream<String> resData = data.connect(dimData).flatMap(new CoFlatMapFunction<String,HashMap<String,String>,String>() {
//          存储国家和大区的关系
            HashMap<String,String>  allMap = new HashMap<String,String>();

//            flatmap1处理的是Kafka的数据
            @Override
            public void flatMap1(String value, Collector out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
//              通过国家获取大区
                String area = allMap.get(countryCode);
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for(int i = 0; i<jsonArray.size();i++){
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                    jsonObject1.put("area",area);
                    jsonObject1.put("dt",dt);
                    out.collect(jsonObject1.toJSONString());

                }

            }
//            flatMap2处理的是Redis中的维度数据
            @Override
            public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {
                this.allMap = value;
            }
        });

        String outTopic = "allDataClean";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers","localhost:9092");
//      设置事务超时时间
        outProp.setProperty("transaction.timeout.ms",60000*15+"");



        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
        resData.addSink(myProducer);

        env.execute(DataCleanJava.class.getName());
    }
}
