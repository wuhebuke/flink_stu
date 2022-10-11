package cn.entity.sink

import cn.entity.apiTest.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从kafka读数据
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","single03:9092")
    prop.setProperty("group_id","sensor_group")
    prop.setProperty("auto.offset.reset","latest")
    val stream1: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), prop))

    //将读出来的数据进行处理，SensorReading转换为String
    val dataStream: DataStream[String] = stream1.map(data => {
      val splits = data.split(",")
      SensorReading(splits(0)+",k2k", splits(1).toLong, splits(2).toDouble).toString
    })

    //将datastream流输出到另一个kafka(sensor2)中
    dataStream.addSink(
      new FlinkKafkaProducer[String]("single03:9092","sensor2",
        new SimpleStringSchema()))

    env.execute("kafka sink stu")
  }

}
