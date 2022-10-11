package cn.entity.sink

import org.apache.flink.streaming.api.scala._
import cn.entity.apiTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从kafka读数据
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","single03:9092")
    prop.setProperty("group_id","sensor_group")
    prop.setProperty("auto.offset.reset","latest")
    val stream1: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), prop))

    //将读出来的数据进行处理
    val dataStream: DataStream[SensorReading] = stream1.map(data => {
      val splits = data.split(",")
      SensorReading(splits(0)+",k2k", splits(1).toLong, splits(2).toDouble)
    })


    //将处理后的数据写出到自定义的sink中
    dataStream.addSink(new MyJdbcSink())

    env.execute("jdbc sink stu")

  }
}

// RickSinkFunction涉及生命周期，涉及上下文
class MyJdbcSink extends RichSinkFunction[SensorReading]{
  var connection:Connection= _
  var insertStat:PreparedStatement= _
  var updateStat:PreparedStatement= _

  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection("jdbc:mysql://single03:3306/flinkSink", "root", "root")
    insertStat = connection.prepareStatement("insert into sensor_temp(id,temp) value (?,?)")
    updateStat = connection.prepareStatement("update sensor_temp set temp=? where id=?")

  }

  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
    updateStat.setDouble(1,value.temperature)
    updateStat.setString(2,value.id)
    updateStat.execute()
    if (updateStat.getUpdateCount==0){
        insertStat.setString(1,value.id)
        insertStat.setDouble(2,value.temperature)
        insertStat.execute()
    }
  }

  override def close(): Unit = {
    insertStat.close()
    updateStat.close()
    connection.close()
  }

}

