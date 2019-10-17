package com.dafy;

import com.alibaba.fastjson.JSONObject;
import com.dafy.Bean.ReportDeptBean;
import com.dafy.RedisMapper.MyRedisMapper;
import com.dafy.sink.ESSink;
import com.dafy.sink.MysqlLateSink;
import com.dafy.sink.MysqlSink;
import com.dafy.watermark.IntentReportWaterMark;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataCleanReport {

    Logger logger = LoggerFactory.getLogger(DataCleanReport.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//       设置使用EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String inTopic = "intent_t2";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        prop.setProperty("group.id","report1");


//        创建Redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
//        创建RedisSink
        RedisSink<Tuple3<String,String,String>> redisSink = new RedisSink<>(conf,new MyRedisMapper());
//       kafka消费配置
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(inTopic,new SimpleStringSchema(),prop);


        /**
         * 获取Kafka中的数据
         *
         *{"nborrowmode":240,"strdeptcode":"034201701","busiAreaCode":"030000010","nstate":5,
         *  "strloandate":1514708501000,"userid":2261687,"adminAreaCode":"034300020","adminAreaName":"两湖区域",
         *  "lamount":1807303,"deptname":"微金武汉市营业部","busiAreaName":"华中中心"}
         */

        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer);
        DataStream<ReportDeptBean> mapData = kafkaData.map(new MapFunction<String,ReportDeptBean>() {
            long eventTime = 0;
            String deptCode="";
            String deptName = "";
            String busiAreaCode = "";
            String busiAreaName = "";
            String adminAreaCode = "";
            String adminAreaName = "";
            String fundcode = "";
            Integer lendCnt = 0;
            Integer lamount = 0;

            ReportDeptBean kafkaDataBean  ;

            @Override
            public ReportDeptBean map(String kafkaValue) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(kafkaValue);

                eventTime = jsonObject.getLong("strloandate");
                deptCode = jsonObject.getString("strdeptcode");
                deptName = jsonObject.getString("detpname");
                busiAreaCode = jsonObject.getString("busiAreaCode");
                busiAreaName = jsonObject.getString("busiAreaName");
                adminAreaCode = jsonObject.getString("adminAreaCode");
                adminAreaName = jsonObject.getString("adminAreaName");
                fundcode = jsonObject.getString("nborrowmode");
                lamount = jsonObject.getInteger("lamount");

                kafkaDataBean = new ReportDeptBean(eventTime,deptCode,deptName,busiAreaCode,busiAreaName,adminAreaCode,adminAreaName,fundcode,lendCnt,lamount);
                return kafkaDataBean;
            }
        });
//    过滤时间异常的数据
        DataStream<ReportDeptBean> filterData = mapData.filter(new FilterFunction<ReportDeptBean>() {
            @Override
            public boolean filter(ReportDeptBean value) throws Exception {
                boolean flag = true;
                if(value.getEventTime() == 0){
                    flag = false;
                }
                return flag;
            }
        });
//保存迟到太久的数据
         OutputTag<ReportDeptBean> outputTag = new OutputTag<ReportDeptBean>("late-data"){};

        /**
         * 窗口统计
         */
        SingleOutputStreamOperator<ReportDeptBean> resultData = filterData.assignTimestampsAndWatermarks( new IntentReportWaterMark())
                .keyBy(ReportDeptBean::getDeptCode)//些处定义了统计分组的字段
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))//允许迟到5秒
                .sideOutputLateData(outputTag)//记录迟到太久的数据
                .apply(new WindowFunction<ReportDeptBean, ReportDeptBean,String, TimeWindow>() {
                    @Override
                    public void apply(String strkey, TimeWindow timeWindow, Iterable<ReportDeptBean> inputVal, Collector<ReportDeptBean> out) throws Exception {
                       //获取分组字段信息
                        String deptcode = strkey;
//                      存储时间，获取最后数据的时间
                        ArrayList<Long> arrayList = new ArrayList();
                        int lendCnt = 0;//借款笔数
                        int lendAmt = 0;//借款金额
                        String deptName = "";
                        String busiAreaCode = "";
                        String busiAreaName = "";
                        String adminAreaCode = "";
                        String adminAreaName = "";
                        String fundcode = "";

                        Iterator<ReportDeptBean> it =inputVal.iterator();
                        while (it.hasNext()){
                            ReportDeptBean  next = it.next();
                            arrayList.add(next.getEventTime());
                            deptName = next.getDeptName();
                            busiAreaCode = next.getBusiAreaCode();
                            busiAreaName = next.getBusiAreaName();
                            adminAreaCode = next.getAdminAreaCode();
                            adminAreaName = next.getAdminAreaName();
                            fundcode = next.getFundcode();
                            lendCnt++;//计算借款笔数
                            lendAmt+=next.getLamount();//计算借款金额
//System.out.println( "统计时间->" + next.getEventTime() + "  lendAmt = " + next.getLamount());
                        }

//System.out.println(Thread.currentThread().getId() + "窗口触发 Count--> " + count);
//                      对时间List排序
                        Collections.sort(arrayList);
                        arrayList.get(arrayList.size() - 1);
                         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String evtime = sdf.format(new Date(arrayList.get(arrayList.size() - 1)));
                        Long leventtime = arrayList.get(arrayList.size() - 1);
//                      组装结果

//System.out.println("统计时间-> " + evtime +  " 营业部->" + deptcode + " " + deptName
//                    + " 中心-> " + busiAreaCode + " " + busiAreaName
//                    + " 区域-> " + adminAreaCode + " " + adminAreaName
//                    + " 资方-> " + fundcode
//                    + " 借款笔数-> " + lendCnt + " 借款金额-> " + lendAmt
//                  );


                        ReportDeptBean res = new ReportDeptBean(leventtime,deptcode,deptName,busiAreaCode,busiAreaName,
                                adminAreaCode,adminAreaName,fundcode, lendCnt,lendAmt);
                        out.collect(res);
                    }
                });

        //获取迟到太久的数据
//        DataStream<ReportDeptBean> sideOutput = resultData.getSideOutput(outputTag);
//      将迟到的数据存储到Kafka
//        sideOutput.addSink(new MysqlLateSink());


//      将结果数据写入 Mysql
        resultData.addSink( new MysqlSink());

//      将结果数据写入 ES
// use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<ReportDeptBean> myESSink = new ESSink().getReportDeptBeanBuilder();
// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        //设置批量写数据的缓冲区大小，实际生产环境要调大一些
        myESSink.setBulkFlushMaxActions(1);
        resultData.addSink(myESSink.build());
        env.execute(DataCleanReport.class.getName());
    }

}
