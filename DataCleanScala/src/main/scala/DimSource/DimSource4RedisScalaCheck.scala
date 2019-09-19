package DimSource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

import scala.collection.mutable

class DimSource4RedisScalaCheck extends SourceFunction[mutable.Map[String,Array[String]]]{

  val logger = LoggerFactory.getLogger("MyRedisSourceScala")
  val SLEEP_MILLION = 60000
  var isRunning = true
  var jedis:Jedis =null

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, Array[String]]]) = {
    this.jedis = new Jedis("localhost",6379)
    import scala.collection.JavaConversions.mapAsScalaMap
    var orgMap = mutable.Map[String,String]()
    orgMap = jedis.hgetAll("organization")
    var orgDimMap = mutable.Map[String,Array[String]]()
    for (key <- orgMap.keys.toList) {
      val value = orgMap.get(key).get
      val splits = value.split(",")
      orgDimMap +=(key -> splits)

      ctx.collect(orgDimMap)
    }
  }
  override def cancel() = {
    if(jedis != null){
      jedis.close()
    }
  }


}
