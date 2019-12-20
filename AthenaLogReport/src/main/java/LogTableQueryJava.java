import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.Properties;


/**
 * @ClassName LogTableQueryJava
 * @Description: i注册kafka中的数据为动态表，并进行统计计算
 * @Author Albert
 * Version v0.9
 */
public class LogTableQueryJava {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //      在系统中指定EventTime概念
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//设置并行度为1
        env.setParallelism(1);
//注册StreamSetting
        EnvironmentSettings fssettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
// 注册流表TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,fssettings);


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
                        .field("rowtime",Types.SQL_TIMESTAMP)
                            .rowtime( new Rowtime()
                                    .timestampsFromField("logoptime")//通过字段指定event_time
                                    .watermarksPeriodicBounded(60000)//延迟60秒生成watermark
                                )
                )
//                .inRetractMode()
                .inAppendMode()//指定数据更新模式为AppendMode,即仅交互insert操作更新数据
                .registerTableSource("log_table");//注册表名为log_table
//        String querySql = "select appId,funcName,userName,substring(stropDate,1,10) as actionDT,count(1) as pv " +
//                            "from log_table" +
//                            " group by " +
//                            " HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '20' SECOND )," +
//                            " appId,funcName,userName,substring(stropDate,1,10)"
//                            ;


//                String querySql = "select appId,funcName,userName,substring(stropDate,1,10) as actionDT,count(1) as pv " +
//                            "from log_table" +
//                            " group by " +
//                            " appId,funcName,userName,substring(stropDate,1,10)"
//                            ;
       try {
             String querySql = "select * from log_table";
             Table logTable = tableEnv.sqlQuery(querySql);

//             tableEnv.toRetractStream(logTable,logPOJO.class).print();
             logTable.printSchema();

             tableEnv.toAppendStream(logTable, Row.class).print();
//             rowDataStream.print();

             env.execute(LogTableQueryJava.class.getName());
           } catch (Exception e) {
            e.printStackTrace();
           }



    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class logPOJO {

        public String appId;
        public String funcId;
        public String funcName;
        public String stropDate;
        public String orgCode;
        public String orgName;
        public String userId;
        public String userName;
        public Timestamp rowtime;
    }
}
