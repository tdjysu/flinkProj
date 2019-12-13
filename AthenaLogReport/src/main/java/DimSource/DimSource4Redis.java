package DimSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * 在Redis中初始化维度数据
 *
 *
 * Redis中存储大区与国家的关系,key 为大区，valeur为对应的国家
 *
 * hset areas AREA_US US
 * hset areas AREA_CJ TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 *
 *
 * 需要将大区与国家的关系组装成HashMap
 */

public class DimSource4Redis implements SourceFunction<HashMap<String,String>> {

    private boolean Running = true;
    private static Jedis myjedis = new Jedis("localhost",6379);
    private final long SLEEP_MILLION = 5000;
    private org.slf4j.Logger logger =  LoggerFactory.getLogger(DimSource4Redis.class);


    @Override
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {


        String key = "";
        String value = "";
//        存储所有国家与大区关系的Map
        HashMap<String,String>  keyValueMap = new HashMap<String,String>();

        while (Running){
            try {
                keyValueMap.clear();
                Map<String,String> areas = myjedis.hgetAll("areas");
                for(Map.Entry<String,String> entry:areas.entrySet()){
//                  获取大区
                    key = entry.getKey();
//                  获取国家字符串
                    value = entry.getValue();
//                  对国家字符串通过,切分
                    String[] splitArray = value.split(",");
//                  将切分后的国家做为key,对应的大区做为value，组装成新的Map
                    for(String split:splitArray){
                        keyValueMap.put(split,key);
                    }
                }

                if(keyValueMap.size() > 0 ){
                    ctx.collect(keyValueMap);
                }else {
                    logger.warn("-------------------从Redis中获取的数据为空------------------");
                }

                Thread.sleep(this.SLEEP_MILLION);
            }catch (JedisConnectionException ex){
                logger.error("Redis连接获取异常",ex.getCause());
                this.myjedis = new Jedis("localhost",6379);
            }catch (Exception e){
                logger.error("Source 数据源异常",e.getCause());
            }

        }
    }

    @Override
    public void cancel() {
        this.Running = false;
        if(myjedis != null){
            myjedis.close();
        }
    }
}
