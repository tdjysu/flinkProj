package DimSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class OrgaRedisSourceJava implements SourceFunction<HashMap<String,String[]>> {

    private static String host = "192.168.8.213";
    private static int port = 6379;
    private boolean Running = true;
    private static Jedis myJedis = new Jedis(host,port);
    private final long SLEEP_MILLION = 5000;
    private org.slf4j.Logger logger =  LoggerFactory.getLogger(OrgaRedisSourceJava.class);
    private Map<String,String> redisMapValue = new HashMap<String,String>();
    private String orgString = "";
    private Map<String,String[]> orgDimMap = new HashMap<String,String[]>();
    @Override 
    public void run(SourceContext ctx) throws Exception {
        while (Running) {
            try {
                orgDimMap.clear();
                redisMapValue = myJedis.hgetAll("organization_dim");
                Set set = redisMapValue.keySet();
                Iterator iterator = set.iterator();
                while (iterator.hasNext()) {
                    String deptcode = (String) iterator.next();
                    orgString = redisMapValue.get(deptcode);
                    String[] orgArray = orgString.split(",");
                    orgDimMap.put(deptcode, orgArray);
                }
                if(orgDimMap.size() > 0 ){
                    ctx.collect(orgDimMap);
                }else {
                    logger.warn("-------------------从Redis中获取的数据为空------------------");
                }

                Thread.sleep(60000);
            }catch (JedisConnectionException ex){
                logger.error("Redis连接获取异常",ex.getCause());
                this.myJedis = new Jedis("localhost",6379);
            }catch (Exception e){
                logger.error("Source 数据源异常",e.getCause());
            }

        }
    }
    @Override
    public void cancel() {
        this.Running = false;
        if(myJedis != null){
            myJedis.close();
        }
    }
}
