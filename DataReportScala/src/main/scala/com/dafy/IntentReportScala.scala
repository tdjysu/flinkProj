package com.dafy


import java.text.SimpleDateFormat
import java.util.{ArrayList, Date, Iterator, Properties}

import bean.ReportDeptBean
import com.alibaba.fastjson.{JSON, JSONObject}
import com.dafy.sink.MysqlSink
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.slf4j.LoggerFactory
import com.dafy.watermark.IntentReportWatermarkScala
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting
import java.text.SimpleDateFormat

/**
  *
  */
object IntentReportScala {

  val Logger = LoggerFactory.getLogger("IntentReportScala")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //修改并行度
    env.setParallelism(5)
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
    val inTopic = "intent_t2"
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




    val mapData: DataStream[ReportDeptBean] = kafkaData.map(kafkaline => {
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
      val kafkaDataBean: ReportDeptBean = new ReportDeptBean
      //      解析文本转换JSON
      val jsonObject = JSON.parseObject(kafkaline)

      generateIntentBean(kafkaDataBean, jsonObject)

      kafkaDataBean
})
//过滤异常数据
    val filterData = mapData.filter(new FilterFunction[ReportDeptBean] {
      override def filter(value: ReportDeptBean): Boolean = {
        return value.eventTime >= 0
      }
    })


//保存迟到太久的数据,
//    scala 要引入 org.apache.flink.streaming.api.scala.OutputTag
    val outputTag = new OutputTag[ReportDeptBean]("late-data"){}

    val resultData = filterData.assignTimestampsAndWatermarks( new IntentReportWatermarkScala())
      .keyBy(_.deptCode)//定义分组字段
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))//滚动统计近10秒的数据
      .sideOutputLateData(outputTag)
      .apply(function = new WindowFunction[ReportDeptBean, ReportDeptBean, String, TimeWindow] {
        override def apply(strkey: String, window: TimeWindow, inputVal: Iterable[ReportDeptBean], out: Collector[ReportDeptBean]): Unit = {
          //获取分组字段信息
          val deptcode: String = strkey
          // 存储时间，获取最后数据的时间
          var timeBuf = ArrayBuffer[Long]()
          var lendCnt: Int = 0 //借款笔数
          var lendAmt: Int = 0 //借款金额
          var deptName: String = ""
          var busiAreaCode: String = ""
          var busiAreaName: String = ""
          var adminAreaCode: String = ""
          var adminAreaName: String = ""
          var fundcode: String = ""
          var timeArray:Array[Long] = new Array[Long](0)

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm;ss")
          val it = inputVal.iterator
          while (it.hasNext) {
            val next: ReportDeptBean = it.next
            timeBuf.append(next.eventTime)
            deptName = next.deptName
            busiAreaCode = next.busiAreaCode
            busiAreaName = next.busiAreaName
            adminAreaCode = next.adminAreaCode
            adminAreaName = next.busiAreaName
            fundcode = next.fundcode
            lendCnt += 1 //计算借款笔数
            lendAmt += next.lamount //计算借款金额
// System.out.println( "统计时间->" + next.getEventTime() + "  lendAmt = " + next.getLamount());

          }

          //          对时间排序
          timeArray = timeBuf.toArray
          Sorting.quickSort(timeArray)
          val evtime = sdf.format(new Date(timeArray.last))
          val levtime = timeArray.last
          //           组织结果数据
          println("统计时间-> " + evtime + " 营业部->" + deptcode + " " + deptName + " 中心-> " + busiAreaCode + " " + busiAreaName + " 区域-> " + adminAreaCode + " " + adminAreaName + " 资方-> " + fundcode + " 借款笔数-> " + lendCnt + " 借款金额-> " + lendAmt)
          var res: ReportDeptBean = getResultIntentData(deptcode, lendCnt, lendAmt, deptName, busiAreaCode, busiAreaName, adminAreaCode, adminAreaName, fundcode, levtime)
          out.collect(res)
        }
      })

    //将结果数据输出到Mysql
      resultData.addSink(new MysqlSink("insert","deptReportAgree"))

      env.execute(IntentReportScala.getClass().getName)

  }

  private def getResultIntentData(deptcode: String, lendCnt: Int, lendAmt: Int, deptName: String, busiAreaCode: String, busiAreaName: String, adminAreaCode: String, adminAreaName: String, fundcode: String, levtime: Long) = {
    var res: ReportDeptBean = new ReportDeptBean()
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
    res
  }

  private def generateIntentBean(kafkaDataBean: _root_.com.dafy.bean.ReportDeptBean, jsonObject: _root_.com.alibaba.fastjson.JSONObject) = {
    kafkaDataBean.eventTime = jsonObject.getLong("strloandate")
    kafkaDataBean.deptCode = jsonObject.getString("strdeptcode")
    kafkaDataBean.deptName = jsonObject.getString("detpname")
    kafkaDataBean.busiAreaCode = jsonObject.getString("busiAreaCode")
    kafkaDataBean.busiAreaName = jsonObject.getString("busiAreaName")
    kafkaDataBean.adminAreaCode = jsonObject.getString("adminAreaCode")
    kafkaDataBean.adminAreaName = jsonObject.getString("adminAreaName")
    kafkaDataBean.fundcode = jsonObject.getString("nborrowmode")
    kafkaDataBean.lamount = jsonObject.getInteger("lamount")
  }
}
