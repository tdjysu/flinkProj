package com.dafy;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.CheckpointingMode;
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

import java.util.Properties;

/**
 * @ClassName DataTableQuery
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class DataTableQuery {
    public static void main(String[] args) {
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        fsEnv.setParallelism(1);
//      注册StreamEnv
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
//      注册流TableEnv
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


//        指定kafka Source
        String topic = "intent_n6";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
        String zookeeperList = "192.168.8.206:2181,192.168.8.207:2181,192.168.8.208:2181";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", brokerList);
        prop.setProperty("group.id", "con1");
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
        fsEnv.enableCheckpointing(5000);//每5秒检查一次
        fsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        fsEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
        fsEnv.getCheckpointConfig().setCheckpointTimeout(60000);
        fsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        fsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        fsTableEnv.connect(kafka)
                .withFormat(
                        new Json().failOnMissingField(true).deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("nborrowmode", Types.STRING)
                                .field("deptcode", Types.STRING)
                                .field("intentID", Types.STRING)
                                .field("busiAreaCode", Types.STRING)
                                .field("nstate", Types.STRING)
                                .field("strloandate", Types.STRING)
                                .field("userid", Types.INT)
                                .field("adminAreaCode", Types.STRING)
                                .field("adminAreaName", Types.STRING)
                                .field("lamount", Types.INT)
                                .field("deptname", Types.STRING)
                                .field("busiAreaName", Types.STRING)
                        .rowtime( new Rowtime()
                                .timestampsFromField("strloandate")//通过字段指定event_time
                                .watermarksPeriodicBounded(60000)//延迟60秒生成WaterMark
                            )
                )

                .inAppendMode()//指定数据更新模式为AppendMode,即仅交互insert操作更新数据
                .registerTableSource("intent_table");//注册表名为intent_table

        String querySql = "select * from intent_table";

        Table intentTable = fsTableEnv.sqlQuery(querySql);

        DataStream<Row> rowDataStream = fsTableEnv.toAppendStream(intentTable, Row.class);
        intentTable.printSchema();
        rowDataStream.print();
        try {
//            table.insertInto("csvOutputTable");
            fsTableEnv.execute(DataTableQuery.class.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
