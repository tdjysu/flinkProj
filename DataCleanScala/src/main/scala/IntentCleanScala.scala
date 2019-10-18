import java.util.Properties

import DimSource.OrgaRedisSourceScala
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011, FlinkKafkaProducer010, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.util.Collector
import scala.collection.mutable

object IntentCleanScala {
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
    val topic = "intent_t1"
    val brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092"
    val prop = new Properties
    prop.setProperty("bootstrap.servers", brokerList)
    prop.setProperty("group.id", "con2")
    //      设置事务超时时间
    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "")

    val myConsumer = new FlinkKafkaConsumer010[String](topic,new SimpleStringSchema(),prop)
    //获取kafka中的数据
    val data = env.addSource(myConsumer)

//   最新的组织机构对应关系
    val mapData = env.addSource(new OrgaRedisSourceScala).broadcast//可以把数据发送到后面的算子的所有并行实例中


//    data.connect(mapData).flatMap(new CoFlatMapFunction[String,] {})
    val resData:DataStream[String] = data.connect(mapData).flatMap(new CoFlatMapFunction[String,mutable.Map[String,Array[String]],String] {
      //存储组织机构维度关系 此变量在两个函数间共享
      var orgaMsp = mutable.Map[String, Array[String]]()

//    处理kafka中的数据,根据营业部编码补充组织机构信息
      override def flatMap1(value: String, out: Collector[String]) = {
          val jSONObject = JSON.parseObject(value)
          val deptcode = jSONObject.getString("strdeptcode")
        //        从Redis中获取大区
        val  orgaArray = orgaMsp.get(deptcode).get
        try{
          for(i <- 0 to orgaArray.length-1){
            val jsonVal = orgaArray(i)
            i match {
              case 0 => jSONObject.put("deptname",jsonVal)
              case 1 => jSONObject.put("busiAreaCode",jsonVal)
              case 2 => jSONObject.put("busiAreaName",jsonVal)
              case 3 => jSONObject.put("adminAreaCode",jsonVal)
              case 4 => jSONObject.put("adminAreaName",jsonVal)
            }
          }
          out.collect(jSONObject.toString)
        }catch{
          case ex:Exception => println("Found Exception-->" + ex)
        }


        println(jSONObject.toString)

      }
//处理Redis中的维度数据
      override def flatMap2(value: mutable.Map[String,Array[String]], out: Collector[String]) = {
        this.orgaMsp = value
      }
    })
    val outTopic = "intent_t2"
    val outProp = new Properties()
    outProp.setProperty("bootstrap.servers", "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092")
    //      设置事务超时时间
    outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "")

    val myProducer = new FlinkKafkaProducer010[String](outTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema), outProp)
    resData.addSink(myProducer)
    //add comment make is lazy
    env.execute("DataCleanScala")
  }

}
