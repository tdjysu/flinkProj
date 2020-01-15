import customerSource.AthenaLogParallSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * @ClassName AthenaLogGener
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class AthenaLogGener {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        try {
              String outTopic = "athena_t1";
              String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
              Properties prop = new Properties();
              prop.setProperty("bootstrap.servers",brokerList);
              prop.setProperty("group.id","ana1");
//设置事务超时时间
              prop.setProperty("transaction.timeout.ms",60000*15+"");

//输出数据配置
              Properties outProp = new Properties();
              outProp.setProperty("bootstrap.servers",brokerList);
              outProp.setProperty("transaction.timeout.ms",60000*15+"");
//将最终结果输出到kafka
              FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
              DataStream  resData = env.addSource( new AthenaLogParallSource());
              resData.addSink(myProducer).setParallelism(3);
              env.execute(AthenaLogGener.class.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
