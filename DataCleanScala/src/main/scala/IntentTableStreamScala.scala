import java.util.Properties

import DimSource.OrgaRedisSourceScala
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import scala.collection.mutable

object IntentTableStreamScala {
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


    //  指定kafka Source
    val topic = "intent_n1"
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
    val resData:DataStream[String] = data.connect(mapData).flatMap(new CoFlatMapFunction[String, mutable.Map[String, Array[String]], String] {
      //存储组织机构维度关系 此变量在两个函数间共享
      var orgaMsp = mutable.Map[String, Array[String]]()

      //    处理kafka中的数据,根据营业部编码补充组织机构信息,并生成新的Table对应的kafka信息
      override def flatMap1(value: String, out: org.apache.flink.util.Collector[String]) = {
        val jSONObject = JSON.parseObject(value)
        val intentJson  = new JSONObject()
        val deptcode = jSONObject.getJSONObject("strdeptcode").getString("value")

        intentJson.put("deptcode",deptcode);
        intentJson.put("intentID", jSONObject.getJSONObject("lid").getString("value"))
        intentJson.put("nborrowmode",jSONObject.getJSONObject("nborrowmode").getString("value" ))
        intentJson.put("strloandate",jSONObject.getJSONObject("strloandate").getString("value"))
        intentJson.put("nstate",jSONObject.getJSONObject("nstate").getString("value"))
        intentJson.put("lamount",jSONObject.getJSONObject("lamount").getInteger("value"))
        intentJson.put("userid",jSONObject.getJSONObject("lborrowerid").getInteger("value"))

        //        从Redis中获取大区
        val orgaArray = orgaMsp.get(deptcode).get
        try {
          for (i <- 0 to orgaArray.length - 1) {
            val jsonVal = orgaArray(i)
            i match {
              case 0 => intentJson.put("deptname", jsonVal)
              case 1 => intentJson.put("busiAreaCode", jsonVal)
              case 2 => intentJson.put("busiAreaName", jsonVal)
              case 3 => intentJson.put("adminAreaCode", jsonVal)
              case 4 => intentJson.put("adminAreaName", jsonVal)
            }
          }
          out.collect(intentJson.toString)
        } catch {
          case ex: Exception => println("Found Exception-->" + ex)
        }


        println(intentJson.toString)

      }

      //处理Redis中的维度数据
      override def flatMap2(value: mutable.Map[String, Array[String]], out: org.apache.flink.util.Collector[String]) = {
        this.orgaMsp = value
      }
    })
    val outTopic = "intent_n6"
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
