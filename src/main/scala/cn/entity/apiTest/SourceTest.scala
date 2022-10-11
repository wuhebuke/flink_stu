package cn.entity.apiTest

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

import java.util.Random

case class SensorReading(id:String,timestamp:Long,temperature:Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建flink环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(2)

    //数据源
    /*val dataList = List(
      1, 2, 3, 4, 5, 6, 21, 12, 344, 65
    )

    val dataList2 = List(
      SensorReading("sensor_1",1665103467,35.8),
      SensorReading("sensor_4",1665103470,18.6),
      SensorReading("sensor_7",1665103473,32.3),
      SensorReading("sensor_9",1665103475,25.5),
    )*/

    //加载数据源 1
    //val stream1: DataStream[Int] = env.fromCollection(dataList)
    //val stream2: DataStream[SensorReading] = env.fromCollection(dataList2)

    //加载数据源 2
    //val stream3: DataStream[Any] = env.fromElements("hello", 1, 1.5, "hello world")

    //加载数据源 3
    /*val path = "D:\\projects\\flink\\flink_stu\\resources\\sensor.txt"
    val stream4: DataStream[String] = env.readTextFile(path)*/

    //加载数据源 4 从服务器端口 7777 取数据
    //[root@master01 ~]# nc -lk 7777
    //val stream5: DataStream[String] = env.socketTextStream("single03", 7777)

    //加载数据源 5 从kafka中取数据
    /*val prop = new Properties()
    prop.setProperty("bootstrap.servers","single03:9092")
    prop.setProperty("group_id","sensor_group")
    prop.setProperty("auto.offset.reset","latest")

    val stream6: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), prop))*/

    //加载数据源 6
    //val stream7: DataStream[SensorReading] = env.addSource(new MySensorSource())

    //控制台输出
    //stream2.print()
    //stream3.print()
    //stream4.print()
    //stream5.print()
    //stream6.print()
    //stream7.print()

    env.execute("source stu")
  }
}

class MySensorSource extends SourceFunction[SensorReading]{
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random = new Random()
    while (true){
      val sensor = SensorReading("sensor_1", System.currentTimeMillis(), random.nextDouble() * 100)
      ctx.collect(sensor)
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
  }
}

