package DimSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName FuncMysqlSourceJava
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class FuncMysqlSourceJava extends  RichParallelSourceFunction<HashMap<String,String>> {

    private boolean Running = true;
    private PreparedStatement ps;
    private Connection connection;
    private org.slf4j.Logger logger =  LoggerFactory.getLogger(FuncMysqlSourceJava.class);
    private Map<String,String> orgDimMap = new HashMap<String,String>();



    // 用来建立连接
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sql = "select func_id,func_name from UAC_SYS_FUNC";
        ps = this.connection.prepareStatement(sql);
//System.out.println("open");
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (Running) {
            try {
                orgDimMap.clear();
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String func_id = resultSet.getString("func_id");
                    String func_name = resultSet.getString("func_name");
                    orgDimMap.put(func_id, func_name);
                }
                sourceContext.collect(orgDimMap);
                Thread.sleep(60000);
            }catch (Exception ex){
                logger.error("Mysql功能维度数据获取异常",ex.getCause());
            }

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
            String url = "jdbc:mysql://192.168.8.212:3306/datacube_uac?useUnicode=true&characterEncoding=UTF-8";
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
