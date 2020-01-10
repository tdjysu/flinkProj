package ResultDataSink;

import DataEntity.LogQueryEntity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class LogQueryEntityMysqlSink extends RichSinkFunction<Tuple2<Boolean,LogQueryEntity>> {

    private Connection connection;
    private PreparedStatement ps;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String upsertSql = "replace into logReport(actionDT,appId,funcId,funcName,orgCode,orgName,logPV,logUV,optime) values(?,?,?,?,?,?,?,?,?)";
        ps = this.connection.prepareStatement(upsertSql);
    }

    @Override
    public void invoke(Tuple2<Boolean,LogQueryEntity> value, Context context) {
       try{
              if(value.f0) {
                  LogQueryEntity logEntity = value.f1;
                  String dayval = logEntity.getActionDT().substring(8,10);
                  //组装数据,执行Upsert操作
                  ps.setString(1, Integer.valueOf(dayval) % 2 +"");
                  ps.setString(2, logEntity.getAppId());
                  ps.setString(3, logEntity.getFuncId());
                  ps.setString(4, logEntity.getFuncName());
                  ps.setString(5, logEntity.getOrgCode());
                  ps.setString(6, logEntity.getOrgName());
                  ps.setLong(7, logEntity.getLogPV());
                  ps.setLong(8, logEntity.getLogUV());
                  ps.setTimestamp(9,new Timestamp(System.currentTimeMillis()));
                  ps.executeUpdate();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null){
            connection.close();
        }
        if(ps != null){
            ps.close();
        }
    }

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://192.168.8.212:3306/bi_app_test?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
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
