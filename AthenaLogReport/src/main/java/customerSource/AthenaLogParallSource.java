package customerSource;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @ClassName AthenaLogParallSource
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class AthenaLogParallSource  implements ParallelSourceFunction<String> {


    private boolean isRunning = true;
    /**
     * 主要的方法，启动一个source，大部分情况下，都需要在这个run方法中实现一个循环，这样就可以不断产生数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning){
            ctx.collect(publishMessage());
//            每秒产生1条数据
            Thread.sleep(1);
        }
    }

    /**
     * 取消一个cancle的时候会调用的方法
     */
    @Override
    public void cancel() {
        this.isRunning = false;
    }


    public String publishMessage() {
        String runtime = new Date().toString();
        DateFormat df= new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        String result = "";
        try {
                int line = 1;
                Date sDate = new Date();
                SimpleDateFormat sdf0 =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String tempString = "{" +
                        "  \"appId\": \"datacube_athena\"," +
                        "  \"bossUserId\": \"" +getRandomUserID()+"\"," +
                        "  \"errorMsg\": \"500ERROR\"," +
                        "  \"funcId\": \""+getRandomFuncID()+"\"," +
                        "  \"funcName\": \"营业部日利润数据统计\"," +
                        "  \"logType\": 1," +
                        "  \"opDate\": \""+df.format(sDate)+"\"," +
                        "  \"opName\": \"logOperation.value(),注解上的值\"," +
                        "  \"orgCode\": \""+getRandomDept()+"\"," +
                        "  \"orgLevel\": 10," +
                        "  \"orgName\": \"大数据中心\"," +
                        "  \"requestInput\": \"JSON.toJSONString(request.getParameterMap()),参数列表\"," +
                        "  \"requestUrl\": \"http://prepare.xdata.dafy.com/athena/report/lend/lenddetail/query4LendDataUpdateTime\"," +
                        "  \"srcIp\": \"\"," +
                        "  \"success\": true," +
                        "  \"supOrgCode\": \"991500000\"," +
                        "  \"supOrgName\": \"云扬达飞\"," +
                        "  \"userId\": \""+getRandomUserID()+"\"," +
                        "  \"userName\": \"张三\"" +
                        "}";
                //解析文本生成Json数据
                JSONObject json = JSONObject.parseObject(tempString);
                String msg = "line " + line++ + " --> " + json.toJSONString();
                result = json.toJSONString();
//System.out.println(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


    public static String getRandomFuncID(){
        String strFuncID ;
        String[] fundArray = {"01b7e04e7bf340c49f1de528411a5ed5","023ba020d6424991b05c570d204f80ea","029f3df98f814f47a3c5b4f9fab332b6",
                "05629eff9bd24b08a904db33aab956a9","07757d3018024e83a2c52c4d965328dd","0a7dc9793382449597d92f3580b1c5cd",
                "0b4be81243f14ef4a7993390a56ede5a","0c5f7bc01b1549cb87e89f8f12dd8198"};
        strFuncID = fundArray[new Random().nextInt(8)];
        return strFuncID;
    }

    public static String getRandomUserID(){
        String strUserD ;
        String[] userArray = {"10135","10138","99d3cb7c61d84f3f952e95a5358d5bd5","7812dccbd6d648d2b6a385301dce1c30","bf4a3cd8926f46259cc160d0d4a5ce5b",
                "0d4cfd3d7b614049b558c7ced997e867","477dbb5cf3ec4a17a302a1b5292584de","fb98af38ac834372af731f58022d9c33","00443e4791cf45b4bbada33af7b83c4a"};
        strUserD = userArray[new Random().nextInt(9)];
        return strUserD;
    }

    public static String getRandomDept(){
        String deptcode = "";
        String[] deptArray = {"114523201","012122931","115327801","021302456","033222102","011516832",
                "012313390","021302836","012101956","021437832","033417601","034222031","021204816",
                "021315681","041303546","115140231","012100111","012101846","034201701","021417762",
                "026122522","012112430","012113560","035305096","041303436","041304566","023734612",
                "033323071","041303816","033501711"};
        deptcode = deptArray[new Random().nextInt(30)];
        return deptcode;
    }
}
