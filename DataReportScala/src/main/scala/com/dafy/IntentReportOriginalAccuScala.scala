package com.dafy

import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.dafy.bean.{ReportDeptBean, ReportOriginalDeptBean}
import com.dafy.sink.MysqlSink
import com.dafy.watermark.IntentReportWatermarkScala
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




    val mapData: DataStream[ReportOriginalDeptBean] = kafkaData.map(kafkaline => {
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
//过滤异常数据
    val filterData: DataStream[ReportOriginalDeptBean] = mapData.filter(new FilterFunction[ReportOriginalDeptBean] {
      override def filter(value: ReportOriginalDeptBean): Boolean = {
        return value.eventTime >= 0 && value.intentState == 9
      }
    })


//保存迟到太久的数据,
//    scala 要引入 org.apache.flink.streaming.api.scala.OutputTag
    val outputTag = new OutputTag[ReportOriginalDeptBean]("late-data"){}

    val resultData = filterData.assignTimestampsAndWatermarks( new IntentReportWatermarkScala())
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

        try {
          //          遍历全部窗口数据
          while (elementNode.hasNext) {
            var next: ReportOriginalDeptBean = elementNode.next()
            lendAmount = next.lamount
            var funder = next.fundcode
            deptName = next.deptName
            busiAreaCode = next.busiAreaCode
            busiAreaName = next.busiAreaName
            adminAreaCode = next.adminAreaCode
            adminAreaName = next.busiAreaName
            fundcode = next.fundcode
            userId = next.userId
            opFlag = next.opFlag
            userCntSate.put(userId, null)

            opFlag match {
              case "U" => {
                lendCntState.update(if(lendCntState.value()== null) 0 else lendCntState.value() + 1)
                lendAmtState.update(if(lendAmtState.value() == null) 0 else lendAmtState.value() + lendAmount)
              }
              case "US" =>{
                lendCntState.update(if(lendCntState.value()== null) 0 else lendCntState.value() - 1)
                lendAmtState.update(if(lendAmtState.value() == null) 0 else lendAmtState.value() - lendAmount)
              }
              case  "I" => {
                lendCntState.update(if(lendCntState.value()== null) 0 else lendCntState.value() + 1)
                lendAmtState.update(if(lendAmtState.value() == null) 0 else lendAmtState.value() + lendAmount)
              }
            }

          }

          var userCount: Integer = 0;
          val userIterator = userCntSate.keys().iterator()
          while (userIterator.hasNext) {
            userIterator.next()
            userCount += 1
          }
          var levtime: Long = System.currentTimeMillis()
          var evtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(levtime)
          //           组织结果数据
println( "线程ID-> " +Thread.currentThread().getId  + " "  + evtime + " 营业部->" + deptcode + " " + deptName + " 中心-> " + busiAreaCode + " " + busiAreaName + " 区域-> " + adminAreaCode + " " + adminAreaName + " 资方-> "
           + fundcode + " 借款笔数-> " + lendCntState.value() + " 借款金额-> " + lendAmtState.value() + " 借款人数-> " + userCount)
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
      resultData.addSink(new MysqlSink("upsert","deptReportAccu"))
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

  private def generateIntentBean(kafkaDataBean: ReportOriginalDeptBean, jsonObject: JSONObject) = {
    kafkaDataBean.eventTime = jsonObject.getJSONObject("strloandate").getLong("value")
    kafkaDataBean.deptCode = jsonObject.getJSONObject("strdeptcode").getString("value")
    kafkaDataBean.deptName = jsonObject.getJSONObject("detpname").getString("value")
    kafkaDataBean.busiAreaCode = jsonObject.getJSONObject("busiAreaCode").getString("value")
    kafkaDataBean.busiAreaName = jsonObject.getJSONObject("busiAreaName").getString("value")
    kafkaDataBean.adminAreaCode = jsonObject.getJSONObject("adminAreaCode").getString("value")
    kafkaDataBean.adminAreaName = jsonObject.getJSONObject("adminAreaName").getString("value")
    kafkaDataBean.fundcode = jsonObject.getJSONObject("nborrowmode").getString("value")
    kafkaDataBean.lamount = jsonObject.getJSONObject("lamount").getInteger("value")
    kafkaDataBean.userId = jsonObject.getJSONObject("userid").getString("value")
    kafkaDataBean.intentState = jsonObject.getJSONObject("nstate").getInteger("value")
    kafkaDataBean.opFlag = jsonObject.getJSONObject("opFlag").getString("value")

    var beforeRecord:JSONObject = jsonObject.getJSONObject("beforeRecord")
    kafkaDataBean.oldDeptCode = beforeRecord.getJSONObject("strdeptcode").getString("value")
    kafkaDataBean.oldDeptName = beforeRecord.getJSONObject("detpname").getString("value")
    kafkaDataBean.oldBusiAreaCode = beforeRecord.getJSONObject("busiAreaCode").getString("value")
    kafkaDataBean.oldBusiAreaName = beforeRecord.getJSONObject("adminAreaName").getString("value")
    kafkaDataBean.oldAdminAreaCode = beforeRecord.getJSONObject("adminAreaCode").getString("value")
    kafkaDataBean.oldAdminAreaName = beforeRecord.getJSONObject("adminAreaName").getString("value")
    kafkaDataBean.oldfunCode = beforeRecord.getJSONObject("nborrowmode").getString("value")
    kafkaDataBean.oldLamount = beforeRecord.getJSONObject("lamount").getInteger("value")
    kafkaDataBean.oldIntentState = beforeRecord.getJSONObject("nstate").getInteger("value")

  }
}