

import DimSource.FuncMysqlSingleSourceJava;
import DimSource.OrgaRedisSourceJava;
import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName LogCleanJava
 * @Description:清洗雅典娜平台点击日志数据，并补充相关维度数据
 * @Author Albert
 * Version v0.9
 */
public class LogAsyncIOSideRedis {

    final static MapStateDescriptor<String, Map> funcs_map = new MapStateDescriptor<String, Map>(
            "dims_map",
            BasicTypeInfo.STRING_TYPE_INFO,
            new MapTypeInfo(String.class,String.class)
    );


    final static MapStateDescriptor<String, String[]> org_map = new MapStateDescriptor<String, String[]>(
            "org_map",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(String[].class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "athena_t1";
        String outTopic = "athena_o1";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        prop.setProperty("group.id","ana1");
//设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");

//输出数据配置
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers",brokerList);
        outProp.setProperty("transaction.timeout.ms",60000*15+"");




        //checkpoint配置
//        env.enableCheckpointing(5000);//每5秒检查一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//设置kafka消费者
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<String>(topic,new SimpleStringSchema(),prop);
//       从最新数据开始消费
        kafkaConsumer.setStartFromLatest();
//获取原生kafka中的数据
        DataStream<String> kafkalog = env.addSource(kafkaConsumer).setParallelism(1);
//从Mysql中获取功能维度数据
        BroadcastStream<Map<String,Map>>funcDim = env.addSource(new FuncMysqlSingleSourceJava()).setParallelism(1).broadcast(funcs_map);
//从Redis中获取组织维度数据
        BroadcastStream <Map<String,String[]>> orgDim = env.addSource(new OrgaRedisSourceJava()).setParallelism(1).broadcast(org_map);

        AsyncRedisFunction asyncRedis = new AsyncRedisFunction();



// 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
        SingleOutputStreamOperator<String> resData = kafkalog.connect(funcDim).process(new ControlFunctionProcess());

        DataStream<String> fresult = AsyncDataStream.orderedWait(resData,asyncRedis, 1000000L, TimeUnit.MILLISECONDS,20).setParallelism(1);

//                .connect(orgDim).process(new ControlOrgaProcess());

fresult.print("cleanAsync-->");


//将最终结果输出到kafka
//        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
//        fresult.addSink(myProducer).setParallelism(3);
        env.execute(LogAsyncIOSideRedis.class.getName());
    }


//  kafka日志补充功能维度数据
    private static class ControlFunctionProcess extends BroadcastProcessFunction<String, Map<String,Map>, String>{
        private MapStateDescriptor  dimsMapStateDescriptor =  new MapStateDescriptor(
                "dims_map",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(Map.class));

        @Override
        public void processElement(String input1_value, ReadOnlyContext ctx, Collector<String> out)  {
            try {
                ReadOnlyBroadcastState<String, Map> dimMap = ctx.getBroadcastState(dimsMapStateDescriptor);
//System.out.println( "DimMap.size->" +  dimMap.l);
                JSONObject originalJSON = JSONObject.parseObject(input1_value);
                String appId= originalJSON.getString("appId");
                String userId = originalJSON.getString("userId");
                String userName= dimMap.get("userMap") == null ? "":dimMap.get("userMap").get(userId).toString();
                String funcId = originalJSON.getString("funcId");
                String orgCode = originalJSON.getString("orgCode");
                String orgName = "";
                String stropDate = originalJSON.getString("opDate");
                String funcName =  dimMap.get("funcMap") == null ? "":dimMap.get("funcMap").get(funcId).toString();
                JSONObject jsondata = geneJSONData(appId,funcId,funcName,stropDate,orgCode,orgName,userId,userName);
//System.out.println(jsondata.toJSONString());
                out.collect(jsondata.toJSONString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void processBroadcastElement(Map<String,Map> mapValue, Context context, Collector<String> collector) throws Exception {
//System.out.println("processBroadcastElement is running ");
            BroadcastState<String, Map> dimMap = context.getBroadcastState(dimsMapStateDescriptor);
            for (Map.Entry<String, Map> entry : mapValue.entrySet()) {
                dimMap.put(entry.getKey(), entry.getValue());
            }

        }
    }


    private static class AsyncRedisFunction extends RichAsyncFunction<String,String>{
        private transient RedisClient redisClient;
        @Override
        public void asyncInvoke(String input1_value, ResultFuture<String> resultFuture) throws Exception {
            JSONObject cleanJSON = JSONObject.parseObject(input1_value);


            redisClient.hgetall("organization_dim",getRes->{
                if(getRes.succeeded()){
                    JsonObject orgJson= getRes.result();
                    if(!orgJson.isEmpty()){
                        String appId= cleanJSON.getString("appId");
                        String userId = cleanJSON.getString("userId");
                        String userName= cleanJSON.getString("userName");
                        String funcId = cleanJSON.getString("funcId");
                        String orgCode = cleanJSON.getString("orgCode");
                        String orgVal = orgJson.getString(orgCode);
                        String[] orgArray = new String[0];
                        if(!orgVal.isEmpty()){
                            orgArray = orgVal.split(",");

                        }

                        String orgName =   orgArray.length > 0 ? orgArray[0] :"" ;
                        String stropDate = cleanJSON.getString("stropDate");
                        String funcName = cleanJSON.getString("funcName");


                        JSONObject cleanJSONData = geneJSONData(appId,funcId,funcName,stropDate,orgCode,orgName,userId,userName);
//System.out.println(cleanJSON.toJSONString());
                        resultFuture.complete(Collections.singleton(cleanJSONData.toJSONString()));

                    }
                }else if(getRes.failed()){
                    resultFuture.complete(Collections.singleton(input1_value));
                }

            });
        }

        @Override
        public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            String host = "127.0.0.1";
            int port = 6379;

            RedisOptions config = new RedisOptions();
            config.setHost(host);
            config.setPort(port);

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);
            vo.setWorkerPoolSize(20);

            Vertx vertx = Vertx.vertx(vo);
            redisClient = RedisClient.create(vertx, config);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if(redisClient!=null)
                redisClient.close(null);
        }
    }


    //补充完功能维度数据后，再通过Redis数据补充组织机构数据
    private static class ControlOrgaProcess extends BroadcastProcessFunction<String, Map<String, String[]>, String>{
        private MapStateDescriptor<String, String[]> dimsMapStateDescriptor =  new MapStateDescriptor<String, String[]>(
                "org_map",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(String[].class)
        );




        @Override
        public void processElement(String input1_value, ReadOnlyContext ctx, Collector<String> out)  {
            try {
                ReadOnlyBroadcastState<String, String[]> orgDimMap = ctx.getBroadcastState(dimsMapStateDescriptor);
                JSONObject cleanJSON = JSONObject.parseObject(input1_value);
                String appId= cleanJSON.getString("appId");
                String userId = cleanJSON.getString("userId");
                String userName= cleanJSON.getString("userName");
                String funcId = cleanJSON.getString("funcId");
                String orgCode = cleanJSON.getString("orgCode");
                String orgName =   orgDimMap.get(orgCode) == null ? "" :orgDimMap.get(orgCode)[0];
                String stropDate = cleanJSON.getString("stropDate");
                String funcName = cleanJSON.getString("funcName");

                JSONObject cleanJSONData = geneJSONData(appId,funcId,funcName,stropDate,orgCode,orgName,userId,userName);
//System.out.println(cleanJSON.toJSONString());
                out.collect(cleanJSONData.toJSONString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void processBroadcastElement(Map<String, String[]> mapValue, Context context, Collector<String> collector) throws Exception {
//System.out.println("processBroadcastElement is running ");
            BroadcastState<String, String[]> orgDimMap = context.getBroadcastState(dimsMapStateDescriptor);
            for (Map.Entry<String, String[]> entry : mapValue.entrySet()) {
                orgDimMap.put(entry.getKey(), entry.getValue());
            }
        }
    }


//  根据日志数据组装JSON
    public static JSONObject geneJSONData(String appID,String funcId,String funcName,String stropDate,String orgCode,String orgName,
                                           String userId,String userName ){
        JSONObject jsonobj = new JSONObject(new LinkedHashMap<>());
        try {
            jsonobj.put("appId",appID);
            jsonobj.put("funcId",funcId);
            jsonobj.put("funcName",funcName);
            jsonobj.put("stropDate",stropDate);
            jsonobj.put("orgCode",orgCode);
            jsonobj.put("orgName",orgName);
            jsonobj.put("userId",userId);
            jsonobj.put("userName",userName);
            SimpleDateFormat UTC_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            UTC_format.setTimeZone(TimeZone.getTimeZone("UTC"));
            SimpleDateFormat dayformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


            Date day = dayformat.parse(stropDate);
            String str = UTC_format.format(day);
            jsonobj.put("logoptime", str);
        } catch (Exception e){
            e.printStackTrace();
        }


        return  jsonobj;
    }

}
