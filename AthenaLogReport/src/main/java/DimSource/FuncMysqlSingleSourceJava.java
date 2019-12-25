package DimSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName FuncMysqlSourceJava
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class FuncMysqlSingleSourceJava extends RichParallelSourceFunction<Map> {

    private boolean Running = true;
    private PreparedStatement funcps;
    private PreparedStatement userps;
    private Map<String,String> funcDimMap = new HashMap<String,String>();
    private Map<String,String> userDimMap = new HashMap<String,String>();
    private Connection connection;
    private org.slf4j.Logger logger =  LoggerFactory.getLogger(FuncMysqlSingleSourceJava.class);
    private Map<String,Map<String,String>> resultMap = new HashMap<String,Map<String,String>>();

    // 用来建立连接
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String funcsql = "select func_id,func_name from UAC_SYS_FUNC";
        String usersql = "SELECT user_id,user_name FROM UAC_USER WHERE user_id IS NOT NULL";
        funcps = this.connection.prepareStatement(funcsql);
        userps = this.connection.prepareStatement(usersql);
//System.out.println("open");
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
            try {
                while (Running) {
                    funcDimMap.clear();
                    ResultSet funcResultSet = funcps.executeQuery();
                    while (funcResultSet.next()) {
                        String func_id = funcResultSet.getString("func_id");
                        String func_name = funcResultSet.getString("func_name");
                        funcDimMap.put(func_id, func_name);
                    }

                    userDimMap.clear();
                    ResultSet userResultSet = userps.executeQuery();
                    while (userResultSet.next()){
                        String user_id = userResultSet.getString("user_id");
                        String user_name = userResultSet.getString("user_name");
                        userDimMap.put(user_id,user_name);
                    }
                    resultMap.clear();
                    resultMap.put("funcMap",funcDimMap);
                    resultMap.put("userMap",userDimMap);
                    sourceContext.collect(resultMap);
                     Thread.sleep(60000);
                }
            }catch (Exception ex){
                logger.error("Mysql功能维度数据获取异常",ex.getCause());
            }finally {
                connection.close();
            }

    }
    @Override
    public void cancel() {
        this.Running = false;
//        if(myJedis != null){
//            myJedis.close();
//        }
    }

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://192.168.8.212:3306/datacube_uac?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
            String username = "root";
            String password = "Root@1234";
            conn = DriverManager.getConnection(url,username,password);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return conn;
    }
}
