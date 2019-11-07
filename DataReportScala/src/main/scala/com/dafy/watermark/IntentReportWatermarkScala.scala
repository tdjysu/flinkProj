package com.dafy.watermark

import com.dafy.bean.ReportOriginalDeptBean
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark


/**
  * @ClassName IntentReportWatermark
  * @Description:TODO
  * @Author Albert
  *         Version v0.9
  */
class IntentReportWatermarkScala extends AssignerWithPeriodicWatermarks[ReportOriginalDeptBean]{
  var currentMaxTimestamp = 0L
     var maxOutOfOrderness = 10000L //最大允许的乱序时间是10秒

  override def extractTimestamp(element: ReportOriginalDeptBean, previousElementTimestamp: Long): Long = {
    val currentTimestamp = element.eventTime
    currentMaxTimestamp = Math.max(currentTimestamp,currentMaxTimestamp)
    currentTimestamp
  }

  override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

}
