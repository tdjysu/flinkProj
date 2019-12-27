
import DataEntity.LogPVEntity;
import ResultDataSink.LogPVEntityMysqlSink;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Properties;


/**
 * @ClassName LogTableQueryJava
 * @Description: i注册kafka中的数据为动态表，并进行统计计算
 * @Author Albert
 * Version v0.9
 */
public class LogTablePVJava {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //      在系统中指定EventTime概念
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//设置并行度为1
        env.setParallelism(1);
//注册StreamSettinga
        EnvironmentSettings fssettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

// 注册流表TableEnv
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,fssettings);
//
//        指定kafka Source
        String topic = "athena_o1";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
        String zookeeperList = "192.168.8.206:2181,192.168.8.207:2181,192.168.8.208:2181";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", brokerList);
        prop.setProperty("group.id", "cont1");
//      设置事务超时时间
        prop.setProperty("transaction.timeout.ms", 60000 * 15 + "");

        Kafka kafka = new Kafka()
                .version("0.10")
                .topic(topic)
                .property("bootstrap.servers", brokerList)
                .property("zookeeper.connect", zookeeperList)
                .property("transaction.timeout.ms", 60000 * 15 + "")
                .property("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .property("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .property("group.id", "test6")
                .startFromLatest();

        //      checkpoint配置
        env.enableCheckpointing(5000);//每5秒检查一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        tableEnv.connect(kafka).withFormat( new Json().deriveSchema())
                .withSchema(
                        new Schema()
                        .field("appId", Types.STRING)
                        .field("funcId",Types.STRING)
                        .field("funcName",Types.STRING)
                        .field("stropDate",Types.STRING)
                        .field("orgCode",Types.STRING)
                        .field("orgName",Types.STRING)
                        .field("userId",Types.STRING)
                        .field("userName",Types.STRING)
                        .field("rowtime",Types.SQL_TIMESTAMP)//  格式"2019-12-24T09:29:45.000Z"
                            .rowtime( new Rowtime()
                                    .timestampsFromField("logoptime")//通过字段指定event_time
                                    .watermarksPeriodicBounded(60000)//延迟60秒生成watermark
                                )
                )
                .inAppendMode()//指定数据更新模式为AppendMode,即仅交互insert操作更新数据
                .registerTableSource("log_table");//注册表名为log_table
        String querySql = "select substring(stropDate,1,10) as actionDT,substring(stropDate,12,5) as actionMinu,appId,funcId,funcName,orgCode,orgName,count(1) as logPV, count(distinct userId) as logUV" +
                            " from log_table" +
                            " group by " +
                            " appId,funcId,funcName,orgCode,orgName,substring(stropDate,1,10),substring(stropDate,12,5)"
                            ;
//
//                String querySql = "select funcName,count(1) as pv " +
//                            "from log_table" +
//                            " group by funcName";
       try {
             Table logTable = tableEnv.sqlQuery(querySql);
//           输出querySql查询结果的表结构
             logTable.printSchema();
//           将querySql的执行结果用Retract的模式打印输出  tableEnv.toRetractStream(logTable,Row.class);
             DataStream rowDataStream = tableEnv.toRetractStream(logTable, LogPVEntity.class);

//             rowDataStream.addSink(new LogPVEntityMysqlSink());
             rowDataStream.print();
             env.execute(LogTablePVJava.class.getName());
           } catch (Exception e) {
            e.printStackTrace();
           }
    }

}
