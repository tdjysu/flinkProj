package Demo;

import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

import java.util.Collections;

/**
 * @ClassName RedisTets
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class RedisTets {

    public static void main(String[] args) {
        RedisClient redisClient;
        String host = "127.0.0.1";  //192.168.8.213
        int port = 6379;

        RedisOptions config = new RedisOptions();
        config.setHost(host);
        config.setPort(port);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(10);
        vo.setWorkerPoolSize(20);

        Vertx vertx = Vertx.vertx(vo);
        redisClient = RedisClient.create(vertx, config);


        redisClient.hgetall("organization_dim",getRes->{

            if(getRes.succeeded()){
                JsonObject orgVal = getRes.result();
                if(!orgVal.isEmpty()){
                    String deptName = orgVal.getString("033323071").split(",")[0];
System.out.println("deptName-->" + deptName);
                }
            }

        });

        if(redisClient!=null)
            redisClient.close(null);
    }
}
