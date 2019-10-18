package com.dafy.sink

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import com.dafy.bean.ReportDeptBean
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * @ClassName MysqlSink
  * @Description:TODO
  * @Author Albert
  *         Version v0.9
  */
class MysqlSink extends  RichSinkFunction [ReportDeptBean]  {

  private var connection:Connection = null
  private var preparedStatement:PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.8.212:3306/xdata?useUnicode=true&characterEncoding=utf8&&allowMultiQueries=true&useSSL=true"
    val username: String = "root"
    val password: String ="Root@1234"

//   加载驱动
    Class.forName(driver)
//    创建连接
    connection = DriverManager.getConnection(url,username,password)
    val sql = "insert into deptReportAgree (cmptimestamp,deptcode,detpname,busiAreaCode,busiAreaName,adminAreaCode,adminAreaName,fundcode,lendCnt,lamount) " + "    values(?,?,?,?,?,?,?,?,?,?)"
    //    获得执行语句
    preparedStatement = connection.prepareStatement(sql)
    }


  override def close(): Unit = {
    if(connection != null ){
      connection.close()
    }
  }

  override def invoke(value: ReportDeptBean): Unit = {
      try{
         preparedStatement.setTimestamp(1, new Timestamp(value.eventTime))
          preparedStatement.setString(2, value.deptCode)
          preparedStatement.setString(3, value.deptName)
          preparedStatement.setString(4, value.busiAreaCode)
          preparedStatement.setString(5, value.busiAreaName)
          preparedStatement.setString(6, value.adminAreaCode)
          preparedStatement.setString(7, value.adminAreaName)
          preparedStatement.setString(8, value.fundcode)
          preparedStatement.setInt(9, value.lendCnt)
          preparedStatement.setInt(10, value.lamount)
        preparedStatement.executeUpdate()
      }catch{
        case e:Exception => println(e.getMessage)
      }
  }

}