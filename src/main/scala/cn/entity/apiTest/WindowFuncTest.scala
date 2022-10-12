package cn.entity.apiTest

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

object WindowFuncTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("single03", 7777)

    /*
    //该情况时间语义简单，wm随事件的有序
    val dataStream1: DataStream[SensorReading] = inputStream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }).assignAscendingTimestamps(x=>x.timestamp)  //数据有序的升序时间语义，即默认数据是有序的延迟为0，wm默认也是有序升序的
    dataStream1.print("info")*/

    val dataStream2: DataStream[SensorReading] = inputStream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    })
      .assignTimestampsAndWatermarks(
        //指定wm生成策略，同时设置延迟时间
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
          .withTimestampAssigner(
            //通过实现接口，指定event(SensorReading)中某个字段作为时间语义进行操作
            new SerializableTimestampAssigner[SensorReading] {
              override def extractTimestamp(t: SensorReading, l: Long): Long = {
                t.timestamp
              }
            }
          )
      )
      /*.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[SensorReading] {
        private val watermark =new Watermark(5000)
        override def getCurrentWatermark: Watermark =
          watermark

        override def extractTimestamp(t: SensorReading, l: Long): Long =
          t.timestamp
      }
    )*/

      /*.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(10)) {
      override def extractTimestamp(t: SensorReading): Long =
        t.timestamp
    })*/

    dataStream2.print("info")


    env.execute("window demo")

  }
}
