package watermark;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;

/**
 * @ClassName AthenaLogWaterMark
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class AthenaLogWaterMark implements AssignerWithPeriodicWatermarks<String> {


    Long currentMaxTimestamp = 0L;
    final Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10秒

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        JSONObject jsonObj = JSONObject.parseObject(element);
        SimpleDateFormat UTC_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Long timestame = 0L;
        try {
            timestame = UTC_format.parse(jsonObj.getString("logoptime")).getTime();
            currentMaxTimestamp = Math.max(timestame, currentMaxTimestamp);
        }catch (Exception e){
            e.printStackTrace();
        }
        return timestame;
    }
}
