package com.dafy.sink

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import com.dafy.bean.{ReportDeptBean, ReportOriginalDeptBean}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * @ClassName MysqlSink
  * @Description:TODO
  * @Author Albert
  *         Version v0.9
  */
class MysqlSink extends  RichSinkFunction [ReportOriginalDeptBean]  {

  private var connection:Connection = null
  private var preparedStatement:PreparedStatement = null
  private var sqlType:String = "insert"
  private var tableName = ""
  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.8.212:3306/xdata?useUnicode=true&characterEncoding=utf8&&allowMultiQueries=true&useSSL=true"
    val username: String = "root"
    val password: String ="Root@1234"

//   加载驱动
    Class.forName(driver)
//    创建连接
    connection = DriverManager.getConnection(url,username,password)
    var sqlPrefix = "insert "
    if(this.sqlType.equals( "upsert")){
      sqlPrefix = "replace"
    }
    val sql = sqlPrefix + " into " +this.tableName+ " (cmptimestamp,deptcode,detpname,busiAreaCode,busiAreaName,adminAreaCode,adminAreaName,fundcode,lendCnt,lamount,userCnt) " + "    values(?,?,?,?,?,?,?,?,?,?,?)"
    //    获得执行语句
    preparedStatement = connection.prepareStatement(sql)
    }


  def this(sqlType:String,tableName:String){
    this()
    this.sqlType = sqlType
    this.tableName = tableName
  }

  override def close(): Unit = {
    if(connection != null ){
      connection.close()
    }
  }

  override def invoke(value: ReportOriginalDeptBean): Unit = {
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
          preparedStatement.setInt(11,value.userCnt)
          preparedStatement.executeUpdate()
         }catch{
            case e:Exception => println(e.getMessage)
      }
  }

}
