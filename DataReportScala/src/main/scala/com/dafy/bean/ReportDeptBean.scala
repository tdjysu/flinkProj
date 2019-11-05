package com.dafy.bean

import scala.beans.BeanProperty


/**
  * @ClassName ReportDeptBean
  * @Description:IntentReportBean
  * @Author Albert
  *         Version v0.9
  */
class ReportDeptBean {
  var eventTime: Long = 0
  var deptCode: String = ""
  var deptName: String = ""
  var busiAreaCode: String = ""
  var busiAreaName: String = ""
  var adminAreaCode: String =""
  var adminAreaName: String =""
  var fundcode: String = ""
  var lendCnt: Integer = 0
  var lamount: Integer = 0
  var userId:String = ""
  var userCnt:Integer = 0
  var intentState:Int = _
  var opFlag:String = _
}
