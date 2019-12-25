import DimSource.FuncMysqlSourceJava;
import DimSource.OrgaRedisSourceJava;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.sun.tools.javadoc.Main.execute;

/**
 * @ClassName LogCleanJava
 * @Description:清洗雅典娜平台点击日志数据，并补充相关维度数据
 * @Author Albert
 * Version v0.9
 */
public class LogCleanJava {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
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
        DataStream kafkalog = env.addSource(kafkaConsumer);
//从Mysql中获取功能维度数据
        DataStream<Map<String, Map<String,String>>> funcDim = env.addSource(new FuncMysqlSourceJava()).setParallelism(1).broadcast();
//从Redis中获取组织维度数据
        DataStream<Map<String,String[]>> orgDim = env.addSource(new OrgaRedisSourceJava()).setParallelism(4).broadcast();

// 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
        DataStream<String> resData = kafkalog.connect(funcDim).flatMap(new FuncControlFunction())
                                      .connect(orgDim).flatMap(new OrgdimControlFunction());

//将最终结果输出到kafka
        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outTopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),outProp);
        resData.addSink(myProducer);
        env.setParallelism(4).execute(LogCleanJava.class.getName());
    }

//将kafka日志数据与功能维度数据关联，补充功能维度数据
    public static class FuncControlFunction extends RichCoFlatMapFunction<String, Map<String,Map<String,String>>, String> {
        Map<String,Map<String,String>> dimMap = new HashMap<String,Map<String,String>>();

        @Override
        public void flatMap1(String input1_value, Collector<String> out) {

//System.out.println( "DimMap.size->" +  dimMap.size());
            JSONObject originalJSON = JSONObject.parseObject(input1_value);
            String appId= originalJSON.getString("appId");
            String userId = originalJSON.getString("userId");
            String userName= dimMap.get("userMap").get(userId);
            String funcId = originalJSON.getString("funcId");
            String orgCode = originalJSON.getString("orgCode");
            String orgName = "";
            String stropDate = originalJSON.getString("opDate");
            String funcName = dimMap.get("funcMap").get(funcId);

            JSONObject jsondata = geneJSONData(appId,funcId,funcName,stropDate,orgCode,orgName,userId,userName);
//System.out.println(jsondata.toJSONString());

            out.collect(jsondata.toJSONString());

        }

        @Override
        public void flatMap2(Map<String, Map<String,String>> dim_value, Collector<String> out) throws Exception {
            Date day=new Date();
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//System.out.println(   df.format(day) + " 当前线程ID-->" +Thread.currentThread().getId()+ " dim_value.size->"+dim_value.size()+"  DimMap.size->" +  dimMap.size());

            this.dimMap = dim_value;
        }
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
        try {
              jsonobj.put("appId",appID);
              jsonobj.put("funcId",funcId);
              jsonobj.put("funcName",funcName);
              jsonobj.put("stropDate",stropDate);
              jsonobj.put("orgCode",orgCode);
              jsonobj.put("orgName",orgName);
              jsonobj.put("userId",userId);
              jsonobj.put("userName",userName);
              SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
              SimpleDateFormat dayformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
              Date day = dayformat.parse(stropDate);
              String str = format.format(day);
              jsonobj.put("logoptime",str);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return  jsonobj;
    }

}
