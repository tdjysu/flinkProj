package com.dafy;

import com.alibaba.fastjson.JSONObject;
import com.dafy.RedisMapper.MyRedisMapper;
import com.dafy.watermark.IntentWaterMark;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataReport {

    Logger logger = LoggerFactory.getLogger(DataReport.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//       设置使用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String inTopic = "intent_t1";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        prop.setProperty("group.id","report1");

//        创建Redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
//        创建RedisSink
        RedisSink<Tuple3<String,String,String>> redisSink = new RedisSink<>(conf,new MyRedisMapper());

        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(inTopic,new SimpleStringSchema(),prop);

        /**
         * 获取Kafka中的数据
         *
         *{"nborrowmode":240,"strdeptcode":"034201701","busiAreaCode":"030000010","nstate":5,
         *  "strloandate":1514708501000,"userid":2261687,"adminAreaCode":"034300020","adminAreaName":"两湖区域",
         *  "lamount":1807303,"deptname":"微金武汉市营业部","busiAreaName":"华中中心"}
         */

        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer);
        DataStream<Tuple3<Long,String,Integer>> mapData = kafkaData.map(new MapFunction<String, Tuple3<Long,String,Integer>>() {
            long eventTime = 0;
            String deptCode="";
            Integer lamount;
            @Override
            public Tuple3<Long, String, Integer> map(String kafkaValue) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(kafkaValue);

                eventTime = jsonObject.getLong("strloandate");
                deptCode = jsonObject.getString("strdeptcode");
                lamount = jsonObject.getInteger("lamount");
                return new Tuple3<Long,String,Integer>(eventTime,deptCode,lamount);
            }
        });
//    过滤时间异常的数据
        DataStream<Tuple3<Long,String,Integer>> filterData = mapData.filter(new FilterFunction<Tuple3<Long, String, Integer>>() {
            @Override
            public boolean filter(Tuple3<Long, String, Integer> value) throws Exception {
                boolean flag = true;
                if(value.f0 == 0){
                    flag = false;
                }
                return flag;
            }
        });
//保存迟到太久的数据
         OutputTag<Tuple3<Long,String,Integer>> outputTag = new OutputTag<Tuple3<Long,String,Integer>>("late-data"){};


        /**
         * 窗口统计
         */
        SingleOutputStreamOperator<Tuple3<String,String,String>> resultData = filterData.assignTimestampsAndWatermarks( new IntentWaterMark())
                .keyBy(1)//些处定义了统计分组的字段
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))//允许迟到5秒
                .sideOutputLateData(outputTag)//记录迟到太久的数据
                .apply(new WindowFunction<Tuple3<Long, String, Integer>, Tuple3<String,String,String>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Long, String, Integer>> inputVal, Collector<Tuple3<String,String, String>> out) throws Exception {
                       //获取分组字段信息
                        String deptcode = tuple.getField(0).toString();
//                      存储时间，获取最后数据的时间
                        ArrayList<Long> arrayList = new ArrayList();
                        long count = 0;

                        Iterator<Tuple3<Long,String,Integer>> it =inputVal.iterator();
                        while (it.hasNext()){
                            Tuple3<Long,String,Integer>  next = it.next();
                            arrayList.add(next.f0);
                            count++;
                        }

//System.out.println(Thread.currentThread().getId() + "窗口触发 Count--> " + count);
//                      对时间List排序
                        Collections.sort(arrayList);
                        arrayList.get(arrayList.size() - 1);
                         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String evtime = sdf.format(new Date(arrayList.get(arrayList.size() - 1)));
//                      组装结果

System.out.println("事件时间--> " +" "+  evtime + "Count--> " + count);
                        Tuple3<String,String,String> res = new Tuple3<>(evtime,deptcode,count+"");
                        out.collect(res);
                    }
                });

        //获取迟到太久的数据
//        DataStream<Tuple3<Long,String,Integer>> sideOutput = resultData.getSideOutput(outputTag);
//      将迟到的数据存储到Kafka
//        sideOutput.addSink();
        resultData.addSink(redisSink);
        env.execute(DataReport.class.getName());
    }
}
