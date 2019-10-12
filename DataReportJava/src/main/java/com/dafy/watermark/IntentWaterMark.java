package com.dafy.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.api.java.tuple.Tuple3;
import javax.annotation.Nullable;

public class IntentWaterMark  implements AssignerWithPeriodicWatermarks<Tuple3<Long,String,Integer>> {


    Long currentMaxTimestamp = 0L;
    final Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10秒

    @Nullable
    @Override
    public Watermark getCurrentWatermark(){
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long,String,Integer> elements,long previousElementTimestamp){
        Long timestame = elements.f0;
        currentMaxTimestamp = Math.max(timestame,currentMaxTimestamp);
        return timestame;
    }

}
