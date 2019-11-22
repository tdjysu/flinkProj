package DimSource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.mutable


class OrgaRedisSourceScala extends SourceFunction[mutable.Map[String,Array[String]]] {
  val logger = LoggerFactory.getLogger("MyRedisSourceScala")
  val SLEEP_MILLION = 60000
  var isRunning = true
  var jedis:Jedis =null

  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, Array[String]]]) = {
    this.jedis = new Jedis("192.168.8.213",6379)
    import scala.collection.JavaConversions.mapAsScalaMap
//  初始化从Redis中获取的Redis Map
    var redis_map = mutable.Map[String,String]()
    //  初始化组织机构维度Map,
    var orga_map = mutable.Map[String,Array[String]]()
    while (isRunning){
      try {
        orga_map.clear()
        redis_map.clear()
//      从Redis中获取组织机构维度Map
        redis_map = jedis.hgetAll("organization_dim")

        for (key <- redis_map.keys.toList) {
          val value = redis_map.get(key).get
          val splits:Array[String] = value.split(",")
          orga_map += (key -> splits)

        }
        if (orga_map.nonEmpty) {
          ctx.collect(orga_map)
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
