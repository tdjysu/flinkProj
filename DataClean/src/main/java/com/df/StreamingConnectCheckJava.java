package com.df;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.df.DimSource.DimSource4Redis;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

public class StreamingConnectCheckJava {

    public static void main(String[] args) throws Exception {
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
        env.enableCheckpointing(5000);//每5秒检查一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        FlinkKafkaConsumer010 myConsumer =  new FlinkKafkaConsumer010<String>(topic,new SimpleStringSchema(),prop);

//      获取kafka中的数据
        DataStream data = env.addSource(myConsumer);

//      从Redis中获取维度数据
        DataStream<HashMap<String, String>> dimData = env.addSource( new DimSource4Redis()).broadcast();
        // 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
       DataStream<String> resData =  data.connect(dimData)
                .flatMap(new ControlFunction());
        String outTopic = "allDataClean";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "localhost:9092");
//      设置事务超时时间
        outProp.setProperty("transaction.timeout.ms",60000*15+"");



        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
        resData.addSink(myProducer);
        env.execute("StreamingConnectCheckJava Job");
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, HashMap<String,String>, String> {
        // key的状态用Boolean值来保存，是被两个流共享的
        // Boolean的blocked用于记住单词是否在control流中，而且这些单词会从streamOfWords流中被过滤掉
        private ValueState<Boolean> blocked;
        private HashMap<String,String> valueMap = new HashMap();
        @Override
        public void open(Configuration config) {
//            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        // control.connect(streamOfWords)顺序决定了control流中的元素会被Flink运行时执行flatMap1时传入处理；streamOfWords流中的元素会被Flink运行时执行flatMap2时传入处理
        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            JSONObject jsonObject = JSONObject.parseObject(control_value);
            String dt = jsonObject.getString("dt");
            String countryCode = jsonObject.getString("countryCode");
//              通过国家获取大区
            String area = valueMap.get(countryCode);
            JSONArray jsonArray = jsonObject.getJSONArray("data");
            for(int i = 0; i<jsonArray.size();i++){
                JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                jsonObject1.put("area",area);
                jsonObject1.put("dt",dt);
                out.collect(jsonObject1.toJSONString());

            }
            System.out.println(control_value);
        }

        @Override
        public void flatMap2(HashMap<String,String> value, Collector<String> out) throws Exception {
//            if (blocked.value() == null) {
//                out.collect(data_value);
//            }
            this.valueMap = value;
        }
    }


}
