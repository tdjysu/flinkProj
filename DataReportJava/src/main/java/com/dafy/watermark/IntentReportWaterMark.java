package com.dafy.watermark;

import com.dafy.bean.ReportDeptBean;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class IntentReportWaterMark implements AssignerWithPeriodicWatermarks<ReportDeptBean> {


    Long currentMaxTimestamp = 0L;
    final Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10秒

    @Nullable
    @Override
    public Watermark getCurrentWatermark(){
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(ReportDeptBean elements, long previousElementTimestamp){
        Long timestame = elements.getEventTime();
        currentMaxTimestamp = Math.max(timestame,currentMaxTimestamp);
        return timestame;
    }

}
