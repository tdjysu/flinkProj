import DimSource.FuncMysqlSourceJava;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

/**
 * @ClassName LogCleanJava
 * @Description:清洗雅典娜平台点击日志数据，并补充相关维度数据
 * @Author Albert
 * Version v0.9
 */
public class LogCleanJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "athena_t1";
        String outTopic = "athena_o1";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        prop.setProperty("group.id","ana1");
//设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");

        Properties outProp = new Properties();


        //      checkpoint配置
        env.enableCheckpointing(5000);//每5秒检查一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//设置kafka消费者
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<String>(topic,new SimpleStringSchema(),prop);
//获取原生kafka中的数据
        DataStream kafkalog = env.addSource(kafkaConsumer);

        DataStream<HashMap<String,String>> funcDim = env.addSource(new FuncMysqlSourceJava()).broadcast();
// 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
        DataStream<String> resData = kafkalog.connect(funcDim).flatMap(new FuncControlFunction())
                                        ;

        outProp.setProperty("bootstrap.servers",brokerList);
        outProp.setProperty("transaction.timeout.ms",60000*15+"");



        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
        resData.addSink(myProducer);
        env.execute(LogCleanJava.class.getName());
    }


    public static class FuncControlFunction extends RichCoFlatMapFunction<String, HashMap<String,String>, String> {

        @Override
        public void flatMap1(String value, Collector<String> out) throws Exception {

        }

        @Override
        public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {

        }
    }

}
