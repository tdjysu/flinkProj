package com.df;

import com.alibaba.fastjson.JSONObject;
import com.df.DimSource.OrgaRedisSourceJava;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

public class IntentConnectJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        指定kafka Source
        String topic = "intent_t1";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
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
//        myConsumer.setStartFromLatest();
//      获取kafka中的数据
        DataStream data = env.addSource(myConsumer);

//      从Redis中获取维度数据
        DataStream<HashMap<String, String[]>> dimData = env.addSource( new OrgaRedisSourceJava()).broadcast();
        // 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
       DataStream<String> resData =  data.connect(dimData)
                .flatMap(new ControlFunction());
        String outTopic = "intent_t2";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092");
//      设置事务超时时间
        outProp.setProperty("transaction.timeout.ms",60000*15+"");



        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
        resData.addSink(myProducer);
        env.execute("StreamingConnectCheckJava Job");
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, HashMap<String,String[]>, String> {
        // key的状态用Boolean值来保存，是被两个流共享的
        // Boolean的blocked用于记住单词是否在control流中，而且这些单词会从streamOfWords流中被过滤掉
        // 营业部维度关系
        HashMap<String,String[]>  orgDimMap = new HashMap<String,String[]>();
        @Override
        public void open(Configuration config) {
//            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        // control.connect(streamOfWords)顺序决定了control流中的元素会被Flink运行时执行flatMap1时传入处理；streamOfWords流中的元素会被Flink运行时执行flatMap2时传入处理
        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
//System.out.println(control_value);
            JSONObject jsonObject = JSONObject.parseObject(control_value);
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
            JSONObject beforeRecord = jsonObject.getJSONObject("beforeRecord");
            String oldDeptcode = null;
            if(beforeRecord != null){
                oldDeptcode = beforeRecord.getString("strdeptcode");
            }
            if(oldDeptcode != null) {
                String oldOrgArray[] = orgDimMap.get(oldDeptcode);
                String olddetpname = oldOrgArray[0];
                String oldbusiAreaCode = oldOrgArray[1];
                String oldbusiAreaName = oldOrgArray[2];
                String oldadminAreaCode = oldOrgArray[3];
                String oldadminAreaName = oldOrgArray[4];
                beforeRecord.put("detpname", olddetpname);
                beforeRecord.put("busiAreaCode", oldbusiAreaCode);
                beforeRecord.put("busiAreaName", oldbusiAreaName);
                beforeRecord.put("adminAreaCode", oldadminAreaCode);
                beforeRecord.put("adminAreaName", oldadminAreaName);
            }
            jsonObject.put("beforeRecord",beforeRecord);
            out.collect(jsonObject.toJSONString());
// System.out.println(jsonObject.toJSONString());

        }

        @Override
        public void flatMap2(HashMap<String,String[]> value, Collector<String> out) throws Exception {
//            if (blocked.value() == null) {
//                out.collect(data_value);
//            }
            this.orgDimMap = value;
        }
    }


}
