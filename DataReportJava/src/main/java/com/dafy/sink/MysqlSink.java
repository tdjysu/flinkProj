package com.dafy.sink;

import com.dafy.Bean.ReportDeptBean;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class MysqlSink implements SinkFunction<ReportDeptBean> {

    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "root";
    String password = "Root@1234";
    String drivername = "com.mysql.jdbc.Driver";
    String dburl = "jdbc:mysql://192.168.8.212:3306/xdata?useUnicode=true&characterEncoding=utf8&&allowMultiQueries=true&useSSL=true";

    @Override
    public void invoke(ReportDeptBean value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "insert into deptReportAgree (cmptimestamp,deptcode,detpname,busiAreaCode,busiAreaName,adminAreaCode,adminAreaName,fundcode,lendCnt,lamount) " +
                "    values(?,?,?,?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setTimestamp(1, new Timestamp(value.getEventTime()));
        preparedStatement.setString(2, value.getDeptCode());
        preparedStatement.setString(3, value.getDeptName());
        preparedStatement.setString(4,value.getBusiAreaCode());
        preparedStatement.setString(5,value.getBusiAreaName());
        preparedStatement.setString(6,value.getAdminAreaCode());
        preparedStatement.setString(7,value.getAdminAreaName());
        preparedStatement.setString(8,value.getFundcode());
        preparedStatement.setInt(9,value.getLendCnt());
        preparedStatement.setInt(10,value.getLamount());
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
}