package com.df.DimSource;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class DimSource4RedisTest {
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
        DataStreamSource source = env.addSource(new OrgaRedisSourceJava());
//        FlinkKafkaConsumer011 myConsumer =  new FlinkKafkaConsumer011<String>(topic,new SimpleStringSchema(),prop);

//      获取kafka中的数据
//        DataStreamSource source = env.addSource(myConsumer);
        source.print();
        env.execute(DimSource4RedisTest.class.getName());

    }
}
