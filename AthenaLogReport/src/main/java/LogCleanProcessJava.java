import DimSource.FuncMysqlSingleSourceJava;
import DimSource.OrgaRedisSourceJava;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName LogCleanJava
 * @Description:清洗雅典娜平台点击日志数据，并补充相关维度数据
 * @Author Albert
 * Version v0.9
 */
public class LogCleanProcessJava {

    final static MapStateDescriptor<String, String> dims_map = new MapStateDescriptor<String, String>(
            "dims_map",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

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


        //      checkpoint配置
        env.enableCheckpointing(5000);//每5秒检查一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);//最小检查间隔 30秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//设置kafka消费者
        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<String>(topic,new SimpleStringSchema(),prop);
//       从最新数据开始消费
        kafkaConsumer.setStartFromLatest();
//获取原生kafka中的数据
        DataStream<String> kafkalog = env.addSource(kafkaConsumer).setParallelism(4);
//从Mysql中获取功能维度数据
        BroadcastStream<Map<String, String>> funcDim = env.addSource(new FuncMysqlSingleSourceJava()).setParallelism(1).broadcast(dims_map);
//从Redis中获取组织维度数据
        DataStream<HashMap<String,String[]>> orgDim = env.addSource(new OrgaRedisSourceJava()).setParallelism(1).broadcast();

// 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
        SingleOutputStreamOperator<String> resData = kafkalog.connect(funcDim).process(new BroadcastProcessFunction<String, Map<String, String>, String>() {
            private MapStateDescriptor<String, String> dimsMapStateDescriptor =  new MapStateDescriptor<String, String>(
                    "dims_map",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            @Override
            public void processElement(String input1_value, ReadOnlyContext ctx, Collector<String> out)  {
                try {
                    ReadOnlyBroadcastState<String, String> dimMap = ctx.getBroadcastState(dimsMapStateDescriptor);
//System.out.println( "DimMap.size->" +  dimMap.l);
                    JSONObject originalJSON = JSONObject.parseObject(input1_value);
                    String appId= originalJSON.getString("appId");
                    String userId = originalJSON.getString("userId");
                    String userName= "";//dimMap.get("userMap").get(userId)"";
                    String funcId = originalJSON.getString("funcId");
                    String orgCode = originalJSON.getString("orgCode");
                    String orgName = "";
                    String stropDate = originalJSON.getString("opDate");
                    String funcName = null;
                    funcName = dimMap.get(funcId);
                    JSONObject jsondata = geneJSONData(appId,funcId,funcName,stropDate,orgCode,orgName,userId,userName);
//System.out.println(jsondata.toJSONString());
                    out.collect(jsondata.toJSONString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void processBroadcastElement(Map<String, String> mapValue, Context context, Collector<String> collector) throws Exception {
//System.out.println("processBroadcastElement is running ");
                BroadcastState<String, String> dimMap = context.getBroadcastState(dimsMapStateDescriptor);
                for (Map.Entry<String, String> entry : mapValue.entrySet()) {
                    dimMap.put(entry.getKey(), entry.getValue());
                }
            }
        });


//将最终结果输出到kafka
        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
        resData.addSink(myProducer);
        env.execute(LogCleanProcessJava.class.getName());
    }


//补充完功能维度数据后，再通过Redis数据补充组织机构数据
    public static class OrgdimControlFunction extends RichCoFlatMapFunction<String,HashMap<String,String[]>,String>{
        HashMap<String,String[]> orgDimMap = new HashMap<String,String[]>();
        @Override
        public void flatMap1(String input1_value, Collector<String> out) throws Exception {
            JSONObject cleanJSON = JSONObject.parseObject(input1_value);
            String appId= cleanJSON.getString("appId");
            String userId = cleanJSON.getString("userId");
            String userName= cleanJSON.getString("userName");
            String funcId = cleanJSON.getString("funcId");
            String orgCode = cleanJSON.getString("orgCode");
            String orgName =   orgDimMap.get(orgCode)[0];;
            String stropDate = cleanJSON.getString("stropDate");
            String funcName = cleanJSON.getString("funcName");

            JSONObject cleanJSONData = geneJSONData(appId,funcId,funcName,stropDate,orgCode,orgName,userId,userName);
//System.out.println(cleanJSON.toJSONString());
            out.collect(cleanJSONData.toJSONString());
        }

        @Override
        public void flatMap2(HashMap<String, String[]> value, Collector<String> out) throws Exception {
            this.orgDimMap = value;
        }
    }


//  根据日志数据组装JSON
    public static JSONObject geneJSONData(String appID,String funcId,String funcName,String stropDate,String orgCode,String orgName,
                                           String userId,String userName ){
        JSONObject jsonobj = new JSONObject(new LinkedHashMap<>());
        jsonobj.put("appID",appID);
        jsonobj.put("funcId",funcId);
        jsonobj.put("funcName",funcName);
        jsonobj.put("stropDate",stropDate);
        jsonobj.put("orgCode",orgCode);
        jsonobj.put("orgName",orgName);
        jsonobj.put("userId",userId);
        jsonobj.put("userName",userName);
        jsonobj.put("eventtime", Timestamp.valueOf(stropDate));

        return  jsonobj;
    }

}