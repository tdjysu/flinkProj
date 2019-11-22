package CustomerSource

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext


/**
  * 创建并行度为1的Source
  * 实现从1开始递增数据
  *
  */
class MyParallelSourceScala extends ParallelSourceFunction[Long]{

  var count = 1L
  var isRunning = true
  override def cancel() = {
     isRunning = false
  }

  override def run(ctx:SourceContext[Long]) = {
    while (isRunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }

  }
}
