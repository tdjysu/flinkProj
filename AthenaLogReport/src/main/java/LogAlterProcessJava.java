

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import watermark.AthenaLogWaterMark;

import java.util.*;

/**
 * @ClassName LogAlterJava
 * @Description:清洗雅典娜平台点击日志数据，并对于1秒内2次及以上点击用户进行报警
 * @Author Albert
 * Version v0.9
 */
public class LogAlterProcessJava {



    final static MapStateDescriptor<String, String[]> org_map = new MapStateDescriptor<String, String[]>(
            "org_map",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(String[].class));

        public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String topic = "athena_o1";

        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        prop.setProperty("group.id","ana1");
//设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");



//        checkpoint配置
        env.enableCheckpointing(5000);//每5秒设置检查点一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);//设置检查点存储超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//设置最大同时进行checkpoint数量
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);//设置checkpoint最小间隔
//      设置外部持久化存储规则 ，DELETE_ON_CANCELLATION 表示手动停止任务时会清理掉checkpoint,  RETAIN_ON_CANCELLATION) 手动停止任务不会清理checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//       设置重启策略 fixedDelayRestart 固定延迟重启，重启3次，每次间隔500毫秒
//            failureRateRestart 失败率重启策略 失败率重启策略在Job失败后会重启，但是超过失败率后，Job会最终被认定失败
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500));


//设置kafka消费者
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<String>(topic,new SimpleStringSchema(),prop);
//       从最新数据开始消费
        kafkaConsumer.setStartFromLatest();
//获取kafka中的数据,并根据用户ID形成KeyedStream
         KeyedStream<String,String> kafkalog = env.addSource(kafkaConsumer).assignTimestampsAndWatermarks(new AthenaLogWaterMark()).setParallelism(3)
                 .keyBy(new KeySelector<String,String>() {
            @Override
            public String getKey(String value) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(value);
                String userID = jsonObj.getString("userId");
                return userID;
            }
        });

        kafkalog.process( new AlterProcess()).print("AlterInfo--> ");

        env.execute(LogAlterProcessJava.class.getName());
    }

    private static String keyStreamFunc() {
        return null;
    }


    private static class AlterProcess extends KeyedProcessFunction<String,String,String> {

        //     定义状态，保存每个用户对应的功能列表
        private MapStateDescriptor funcUserMapStateDescriptor =  new MapStateDescriptor(
                "funcUser_map",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(Map.class));
//定义每个用户对应的报警时间戳
        private ValueStateDescriptor currentTimerValueStateDesciptor =  new ValueStateDescriptor(
                "currentTime",
                TypeInformation.of(Long.class));

        private MapState<String,Map> funcUsersMapState;
        private ValueState<Long> currentTimer;
      @Override
      public void open(Configuration parameters) {
//    从上下文中取出对应的状态值
          funcUsersMapState = getRuntimeContext().getMapState(funcUserMapStateDescriptor);
          currentTimer = getRuntimeContext().getState(currentTimerValueStateDesciptor);
      }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            JSONObject jsonObj = JSONObject.parseObject(value);
            String userID = jsonObj.getString("userId");
            String funcID = jsonObj.getString("funcId");
//            先取出用户上一次点击对应的功能列表
            Map funcsMap = funcUsersMapState.get(userID) == null ? new HashMap():funcUsersMapState.get(userID);
//            将本次用户点击功能加入用户对应的功能列表
            funcsMap.put(funcID,funcID);
//            更新用户对应功能的状态值
            funcUsersMapState.put(userID,funcsMap);
//            取出用户上条记录的定时器的时间戳
            long currentTimerTs = currentTimer.value() == null ? 0 :currentTimer.value();
//           如果获取到的用户对应功能Map大小 > 1且没有注册过定时器 ，则创建定时器
            if(currentTimerTs == 0 && funcsMap.size() > 1 ){
//          定义定时器为1秒,注意参数是时间戳，不是延迟时间长度
                long timerTs = ctx.timerService().currentWatermark() + 1000L;
                ctx.timerService().registerEventTimeTimer(timerTs);
//              更新用户定时器时间戳
                currentTimer.update(timerTs);
            }
//            如果用户对应的功能是相同的，即Map大小为1，或者 第一条数据用户对应功能Map为空，则删除定时器
            else if(funcsMap.size() == 1 || funcsMap.isEmpty()){
                ctx.timerService().deleteEventTimeTimer(currentTimer.value() == null ? 0 :currentTimer.value());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
//          输出报警信息
            out.collect("user_id --> " + ctx.getCurrentKey() + " 被他人盗用");
//System.out.println("user_id --> " + ctx.getCurrentKey() + "被他人盗用");
//          清空状态数据，释放资源
//            funcUsersMapState.clear();
            currentTimer.clear();
        }
    }
}
