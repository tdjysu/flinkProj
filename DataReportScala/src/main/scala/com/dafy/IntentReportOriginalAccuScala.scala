package com.dafy

import java.lang
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.dafy.bean.{ReportDeptBean, ReportOriginalDeptBean}
import com.dafy.sink.{MysqlOriginalSink, MysqlSink}
import com.dafy.watermark.{IntentOriginalWatermarkScala, IntentReportWatermarkScala}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  *
  */
object IntentReportOriginalAccuScala {

  val Logger = LoggerFactory.getLogger("IntentReportScala")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //修改并行度
    env.setParallelism(10)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //checkPoint 设置
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    //隐式转换
    import org.apache.flink.api.scala._

    val inTopic = "intent_n2"
    val brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092"
    val prop = new Properties
    prop.setProperty("bootstrap.servers", brokerList)
    prop.setProperty("group.id", "report1")

    val kafkaConsumer = new FlinkKafkaConsumer010[String](inTopic,new SimpleStringSchema(),prop)
//  设置kafka从最新数据消费
    kafkaConsumer.setStartFromLatest()
    /**
      * 获取Kafka中的数据
      *
      * {"nborrowmode":240,"strdeptcode":"034201701","busiAreaCode":"030000010","nstate":5,
      * "strloandate":1514708501000,"userid":2261687,"adminAreaCode":"034300020","adminAreaName":"两湖区域",
      * "lamount":1807303,"deptname":"微金武汉市营业部","busiAreaName":"华中中心"}
      */
    val kafkaData = env.addSource(kafkaConsumer)




    val mapData: DataStream[ReportOriginalDeptBean] = kafkaData.filter(kafkaline => {
      //      解析文本转换JSON
      val jsonObject: JSONObject = JSON.parseObject(kafkaline)
//      只统计状态，金额，资金方变化或者新插入的数据
          checkDataIsActive(jsonObject)
    }).map(kafkaline => {
      var eventTime: Long = 0
      var deptCode: String = ""
      var deptName: String = ""
      var busiAreaCode: String = ""
      var busiAreaName: String = ""
      var adminAreaCode: String = ""
      var adminAreaName: String = ""
      var fundcode: String = ""
      var lendCnt: Integer = 0
      var lamount: Integer = 0
      val kafkaDataBean: ReportOriginalDeptBean = new ReportOriginalDeptBean
      //      解析文本转换JSON
      val jsonObject = JSON.parseObject(kafkaline)
//    根据Json生成对象
      generateIntentBean(kafkaDataBean, jsonObject)

      kafkaDataBean
})
//过滤状态异常数据
    val filterData: DataStream[ReportOriginalDeptBean] = mapData.filter(new FilterFunction[ReportOriginalDeptBean] {
      override def filter(value: ReportOriginalDeptBean): Boolean = {
        val intentValid:Boolean = value.intentState == 4 || value.intentState == 5 || value.intentState == 7 || value.intentState == 9

        return intentValid
      }
    })


//保存迟到太久的数据,
//    scala 要引入 org.apache.flink.streaming.api.scala.OutputTag
    val outputTag = new OutputTag[ReportOriginalDeptBean]("late-data"){}

    val resultData = filterData.assignTimestampsAndWatermarks( new IntentOriginalWatermarkScala())
      .keyBy(_.deptCode)//定义分组字段
      .window(TumblingEventTimeWindows.of(Time.days(1)))//滚动统计1天的数据
      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))//增加trigger,以一定的频率输出中间结果
      .evictor(TimeEvictor.of(Time.seconds(0),true))//增加evictor是因为，每次trigger触发计算，窗口中的所有数据都会参与，所以数据会
//      触发多次，比较浪费，加evictor驱逐已经计算过的数据,就不会重复计算了
      .sideOutputLateData(outputTag)//处理迟到的数据
      .process(function = new ProcessWindowFunction[ReportOriginalDeptBean, ReportOriginalDeptBean, String, TimeWindow] {
      var lendCntState: ValueState[Integer] = _ //定义借款笔数计算状态变量
      var lendAmtState: ValueState[Integer] = _ //定义借款金额计算状态变量
      var userCntSate: MapState[String, String] = _ //定义借款人数计算状态变量


      override def open(parameters: Configuration): Unit = {
        userCntSate = getRuntimeContext.getMapState(new MapStateDescriptor[String, String]("userCntState", classOf[String], classOf[String]))
        lendCntState = getRuntimeContext.getState[Integer](new ValueStateDescriptor[Integer]("lendCntState", classOf[Integer]))
        lendAmtState = getRuntimeContext.getState[Integer](new ValueStateDescriptor[Integer]("lendAmtState", classOf[Integer]))
      }

      override def process(strkey: String, context: Context, elements: Iterable[ReportOriginalDeptBean], out: Collector[ReportOriginalDeptBean]): Unit = {


        //获取分组字段信息
        val deptcode: String = strkey
        // 存储时间，获取最后数据的时间
        var timeBuf = ArrayBuffer[Long]()
        var deptName: String = ""
        var busiAreaCode: String = ""
        var busiAreaName: String = ""
        var adminAreaCode: String = ""
        var adminAreaName: String = ""
        var fundcode: String = ""
        var timeArray: Array[Long] = new Array[Long](0)
        var lendAmount = 0
        var userId = ""
        val elementNode = elements.iterator
        var opFlag:String = ""
        var oldLamount = 0
        var oldNstate = 0

        try {
          //          遍历全部窗口数据
          while (elementNode.hasNext) {

            var next: ReportOriginalDeptBean = elementNode.next()
            lendAmount = next.lamount
            deptName = next.deptName
            busiAreaCode = next.busiAreaCode
            busiAreaName = next.busiAreaName
            adminAreaCode = next.adminAreaCode
            adminAreaName = next.busiAreaName
            fundcode = next.fundcode
            userId = next.userId
            opFlag = next.opFlag

            oldNstate = next.oldIntentState
            oldLamount = next.oldLamount
            //若修改的历史数据状态为有效的，则减掉有效的历史借款金额，否则减掉的历史借款金额为0
            oldLamount = if(oldNstate == 4 || oldNstate == 5 ||oldNstate == 7 ||oldNstate == 9) oldLamount else 0
            userCntSate.put(userId, null)
            val intent_step = 1
            opFlag match {
              case "U" => {
println( "lendAmtState-->" +lendAmtState.value() + "  lendAmt-->" + lendAmount + "  oldLamt-->" + oldLamount)
                lendCntState.update(if(lendCntState.value()== null) 0 else lendCntState.value() + intent_step)
                lendAmtState.update(if(lendAmtState.value() == null) 0 else lendAmtState.value() + lendAmount - oldLamount)
              }
              case  "I" => {
                lendCntState.update(if(lendCntState.value()== null) 0 else lendCntState.value() + 1)
                lendAmtState.update(if(lendAmtState.value() == null) 0 else lendAmtState.value() + lendAmount)
              }
            }

          }

          var userCount: Integer = 0
          val userIterator = userCntSate.keys().iterator()
          while (userIterator.hasNext) {
            userIterator.next()
            userCount += 1
          }
          var levtime: Long = System.currentTimeMillis()
          var evtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(levtime)
          //           组织结果数据
//println( "线程ID-> " +Thread.currentThread().getId  + " "  + evtime + " 营业部->" + deptcode + " " + deptName + " 中心-> " + busiAreaCode + " " + busiAreaName + " 区域-> " + adminAreaCode + " " + adminAreaName + " 资方-> "
//           + fundcode + " 借款笔数-> " + lendCntState.value() + " 借款金额-> " + lendAmtState.value() + " 借款人数-> " + userCount)
println( "线程ID-> " +Thread.currentThread().getId  + " "  + evtime + " 营业部->" + deptcode + fundcode + " 借款笔数-> " + lendCntState.value() + " 借款金额-> " + lendAmtState.value() + " 借款人数-> " + userCount)
          var res: ReportOriginalDeptBean = getResultIntentData(deptcode, lendCntState.value(), lendAmtState.value(), deptName, busiAreaCode, busiAreaName, adminAreaCode, adminAreaName, fundcode, levtime,userCount)
          out.collect(res)
        }catch{ case  e:Exception => {
          Logger.error("",e.getCause)
          println("Cause-->" + e.getCause);
          println("Message-->" + e.getMessage)
        }
      }
      }
    })


    //将结果数据输出到Mysql
      resultData.addSink(new MysqlOriginalSink("upsert","deptReportAccu"))
      env.execute(IntentReportAccuScala.getClass().getName)

  }

  private def getResultIntentData(deptcode: String, lendCnt: Int, lendAmt: Int, deptName: String, busiAreaCode: String, busiAreaName: String, adminAreaCode: String, adminAreaName: String, fundcode: String, levtime: Long,userCount:Integer) = {
    var res: ReportOriginalDeptBean = new ReportOriginalDeptBean()
    res.eventTime = levtime
    res.deptCode = deptcode
    res.deptName = deptName
    res.busiAreaCode = busiAreaCode
    res.busiAreaName = busiAreaName
    res.adminAreaCode = adminAreaCode
    res.adminAreaName = adminAreaName
    res.fundcode = fundcode
    res.lamount = lendAmt
    res.lendCnt = lendCnt
    res.userCnt = userCount
    res
  }

  private def checkDataIsActive(jsonObject:JSONObject):Boolean ={
    val isActive:Boolean = false
    val isInsert = "I" == jsonObject.getString("opFlag")
    val strDate = new SimpleDateFormat("yyyy-MM-dd").format(jsonObject.getLong("strloandate")).toString
    var loanDateIsActive = "2019-10-30".equals(strDate)
    val intentStateChange: Boolean = jsonObject.getJSONObject("nstate").getBoolean("isupdate")
    val lamountChange = jsonObject.getJSONObject("lamount").getBoolean("isupdate")
    val funderChange = jsonObject.getJSONObject("nborrowmode").getBoolean("isupdate")

    return loanDateIsActive && (intentStateChange || lamountChange || funderChange || isInsert)
  }

  private def generateIntentBean(kafkaDataBean: ReportOriginalDeptBean, jsonObject: JSONObject) = {

    kafkaDataBean.eventTime = jsonObject.getLong("strloandate")
    kafkaDataBean.deptCode = jsonObject.getJSONObject("strdeptcode").getString("value")
    kafkaDataBean.deptName = jsonObject.getString("detpname")
    kafkaDataBean.busiAreaCode = jsonObject.getString("busiAreaCode")
    kafkaDataBean.busiAreaName = jsonObject.getString("busiAreaName")
    kafkaDataBean.adminAreaCode = jsonObject.getString("adminAreaCode")
    kafkaDataBean.adminAreaName = jsonObject.getString("adminAreaName")
    kafkaDataBean.fundcode = jsonObject.getJSONObject("nborrowmode").getString("value")
    kafkaDataBean.lamount = jsonObject.getJSONObject("lamount").getInteger("value")
    kafkaDataBean.userId = jsonObject.getJSONObject("lborrowerid").getString("value")
    kafkaDataBean.intentState = jsonObject.getJSONObject("nstate").getInteger("value")
    kafkaDataBean.opFlag = jsonObject.getString("opFlag")

    var beforeRecord:JSONObject = jsonObject.getJSONObject("beforeRecord")
    if(beforeRecord != null){
      kafkaDataBean.oldDeptCode = beforeRecord.getJSONObject("strdeptcode").getString("value")
      kafkaDataBean.oldDeptName = beforeRecord.getString("detpname")
      kafkaDataBean.oldBusiAreaCode = beforeRecord.getString("busiAreaCode")
      kafkaDataBean.oldBusiAreaName = beforeRecord.getString("adminAreaName")
      kafkaDataBean.oldAdminAreaCode = beforeRecord.getString("adminAreaCode")
      kafkaDataBean.oldAdminAreaName = beforeRecord.getString("adminAreaName")
      kafkaDataBean.oldfunCode = beforeRecord.getJSONObject("nborrowmode").getString("value")
      kafkaDataBean.oldLamount = beforeRecord.getJSONObject("lamount").getInteger("value")
      kafkaDataBean.oldIntentState = beforeRecord.getJSONObject("nstate").getInteger("value")
      kafkaDataBean.oldLoandate = beforeRecord.getLong("strloandate")
    }


  }
}