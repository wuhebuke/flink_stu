package cn.entity.sink

import cn.entity.apiTest.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val path ="D:\\projects\\flink\\flink_stu\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(path)

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val str = data.split(",")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    //dataStream.print()  //特殊的Sink 在控制台输出
//    dataStream.writeAsCsv("D:\\projects\\flink\\flink_stu\\resources\\out.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("D:\\projects\\flink\\flink_stu\\resources\\out1.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("sink stu")

  }

}
