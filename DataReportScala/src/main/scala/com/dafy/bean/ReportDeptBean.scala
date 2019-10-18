package com.dafy.bean

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.BeanProperty

/**
  * @ClassName ReportDeptBean
  * @Description:IntentReportBean
  * @Author Albert
  *         Version v0.9
  */
class ReportDeptBean {
  @BeanProperty var eventTime: Long = 0
  @BeanProperty var deptCode: String = ""
  @BeanProperty var deptName: String = ""
  @BeanProperty var busiAreaCode: String = ""
  @BeanProperty var busiAreaName: String = ""
  @BeanProperty var adminAreaCode: String =""
  @BeanProperty var adminAreaName: String =""
  @BeanProperty var fundcode: String = ""
  @BeanProperty var lendCnt: Integer = 0
  @BeanProperty var lamount: Integer = 0

}
