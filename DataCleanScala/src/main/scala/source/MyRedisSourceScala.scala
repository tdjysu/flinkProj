package source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable


class MyRedisSourceScala extends SourceFunction[mutable.Map[String,String]] {
  val logger = LoggerFactory.getLogger("MyRedisSourceScala")
  val SLEEP_MILLION = 60000
  var isRunning = true
  var jedis:Jedis =null

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, String]]) = {
    this.jedis = new Jedis("localhost",6379)
    import scala.collection.JavaConversions.mapAsScalaMap
    var keyValueMap = mutable.Map[String,String]()
    while (isRunning){
      try {
        keyValueMap.clear()
        keyValueMap = jedis.hgetAll("areas")

        for (key <- keyValueMap.keys.toList) {
          val value = keyValueMap.get(key).get
          val splits = value.split(",")
          for (split <- splits) {
            keyValueMap += (key -> split)
          }
        }
        if (keyValueMap.nonEmpty) {
          ctx.collect(keyValueMap)
        } else {
          logger.warn("The Dim Data from Redis is Empty")
        }
        Thread.sleep(SLEEP_MILLION)
      }catch {
        case  e:JedisConnectionException => {
          logger.error("Redis 链接获取异常,重新获取连接",e.getCause)
          jedis = new Jedis("localhost",6379)
        }
        case  e:Exception => {
          logger.error("Source 数据源异常",e.getCause)
        }
      }
    }
  }

  override def cancel() = {
    isRunning = false
    if(jedis != null){
      jedis.close()
    }
  }
}
