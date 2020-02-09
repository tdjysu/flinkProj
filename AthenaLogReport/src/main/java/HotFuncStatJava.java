import DataEntity.HotFuncItem;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import watermark.AthenaLogWaterMark;

import java.sql.Timestamp;
import java.util.*;

/**
 * @ClassName HotFuncStatJava
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class HotFuncStatJava {

    public static void main(String[] args) {
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
//        WindowedStream kafkalog =
                env.addSource(kafkaConsumer).assignTimestampsAndWatermarks(new AthenaLogWaterMark()).setParallelism(3)
                .keyBy(new KeySelector<String,String>() {//根据功能id分组
                    @Override
                    public String getKey(String value) throws Exception {
                        JSONObject jsonObj = JSONObject.parseObject(value);
                        String funcID = jsonObj.getString("funcId");
                        return funcID;
                    }
                }).timeWindow(Time.minutes(1),Time.seconds(5)) // 窗口长度为60S，5s滚动
                .aggregate(new CountAgg(), new WindowResult())// 自定义预聚合函数
                .keyBy(new KeySelector<HotFuncItem,Long>() {// 根据窗口来分组
                    @Override
                    public Long getKey(HotFuncItem value) throws Exception {
                        return value.getWinEnd();
                    }
                })
                .process(new TopFunc(5))//自定义转换算子
                ;




    }

    private static class CountAgg implements AggregateFunction<String,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(String value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class WindowResult implements WindowFunction<Long, HotFuncItem,String,TimeWindow>  {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<HotFuncItem> out) throws Exception {
            out.collect(new HotFuncItem(key,window.getEnd(),input.iterator().next()));
        }
    }


    private static class TopFunc extends KeyedProcessFunction<Long,HotFuncItem,String> {

        private ListState<HotFuncItem> itemState ;

        int topSize = 5;
        public TopFunc(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.itemState = getRuntimeContext().getListState(new ListStateDescriptor<HotFuncItem>("item-state", TypeInformation.of(HotFuncItem.class)));
        }
        @Override
        public void processElement(HotFuncItem value, Context ctx, Collector<String> out) throws Exception {
            // 把每条数据存入状态列表
            itemState.add(value);
            // 注册一个定时器
            ctx.timerService().registerEventTimeTimer(value.getWinEnd() + 1);
        }


        // 定时器触发时，对所有数据排序，并输出结果
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将所有state中的数据取出，放到一个List Buffer中
            List<HotFuncItem>  allItemList = new ArrayList<>();
            Iterator<HotFuncItem> itr = itemState.get().iterator();
            while(itr.hasNext()){
                allItemList.add(itr.next());
            }
           allItemList.sort((HotFuncItem o1, HotFuncItem o2) -> o1.getCount().compareTo(o2.getCount()));
           allItemList.subList(0,4);
           itemState.clear();
            StringBuffer bf = new StringBuffer();
            bf.append("时间：").append( new Timestamp( timestamp - 1 ) ).append("\n");
           for(int i=0;i<allItemList.size();i++){
                HotFuncItem hf = allItemList.get(i);

               bf.append("第" + i + "名功能 " + hf.getFuncId() + " pv=" + hf.getCount());
               bf.append("\n").append("========================================").append("\n");
            }
            out.collect(bf.toString());
        }


    }
}
