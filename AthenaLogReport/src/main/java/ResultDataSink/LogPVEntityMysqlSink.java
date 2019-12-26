package ResultDataSink;

import DataEntity.LogPVEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class LogPVEntityMysqlSink extends RichSinkFunction<LogPVEntity> {

    private Connection connection;
    private PreparedStatement ps;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String upsertSql = "replace into logPVAggre(actionDT,actionMinu,appId,funcId,funcName,orgCode,orgName,logPV,logUV) values(?,?,?,?,?,?,?,?,?)";
        ps = this.connection.prepareStatement(upsertSql);
    }

    @Override
    public void invoke(LogPVEntity logEntity, Context context) {
        try {
    //        组装数据,执行Upsert操作
                ps.setString(1,logEntity.getActionDT());
                ps.setString(2,logEntity.getActionMinu());
                ps.setString(3,logEntity.getAppId());
                ps.setString(4,logEntity.getFuncId());
                ps.setString(5,logEntity.getFuncName());
                ps.setString(6,logEntity.getOrgCode());
                ps.setString(7,logEntity.getOrgCode());
                ps.setLong(8,logEntity.getLogPV());
                ps.setLong(9,logEntity.getLogUV());
                ps.executeUpdate();
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
