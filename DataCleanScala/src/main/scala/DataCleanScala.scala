import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector
import DimSource.MyRedisSourceScala

import scala.collection.mutable

object DataCleanScala {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//设置并行度为4
    env.setParallelism(4)
//    checkpoint 配置
    env.enableCheckpointing(60000) //每60秒检查一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000) //最小检查间隔 30秒
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    //        指定kafka Source
    import org.apache.flink.api.scala._
    val topic = "allData"
    val brokerList = "localhost:9092"
    val prop = new Properties
    prop.setProperty("bootstrap.servers", brokerList)
    prop.setProperty("group.id", "con2")
    //      设置事务超时时间
    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "")

    val myConsumer = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
    //获取kafka中的数据
    val data = env.addSource(myConsumer)

//   最新的国家码与大区的对应关系
    val mapData = env.addSource(new MyRedisSourceScala).broadcast//可以把数据发送到后面的算子的所有并行实例中

    val resData:DataStream[String] = data.connect(mapData).flatMap(new CoFlatMapFunction[String,mutable.Map[String,String],String] {
      //存储国家和大区的关系 此变量在两个函数间共享
      var allMsp = mutable.Map[String, String]()

//    处理kafka中的数据
      override def flatMap1(value: String, out: Collector[String]) = {
          val jSONObject = JSON.parseObject(value)
          val dt = jSONObject.getString("dt")
          val countryCode = jSONObject.getString("countryCode")
//        从Redis中获取大区
          val area = allMsp.get(countryCode)

          val jsonArray = jSONObject.getJSONArray("data")
          for(i <- 0 to jsonArray.size()){
            val jsonObject1 =jsonArray.getJSONObject(i)
            jsonObject1.put("area",area)
            jsonObject1.put("dt",dt)
            out.collect(jsonObject1.toString)
          }
      }
//处理Redis中的维度数据
      override def flatMap2(value: mutable.Map[String, String], out: Collector[String]) = {
        this.allMsp = value
      }
    })
    val outTopic = "allDataClean"
    val outProp = new Properties()
    outProp.setProperty("bootstrap.servers", "localhost:9092")
    //      设置事务超时时间
    outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "")

    val myProducer = new FlinkKafkaProducer011[String](outTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema), outProp, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)
    resData.addSink(myProducer)
    //add comment make is lazy
    env.execute("DataCleanScala")
  }

}
