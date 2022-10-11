package cn.entity.apiTest

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, ReduceFunction, RichFilterFunction, RichMapFunction, RichReduceFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[String] = env.socketTextStream("single03", 7777)

    //通过map对字符串流进行结构转换
    /*val dataStream: DataStream[SensorReading] =
      stream1.map(data => {
             val strings = data.split(",")
             SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
           })*/
    //以上等效于：(map方法可以定义一个Mapper类)
    val dataStream: DataStream[SensorReading] = stream1.map(new MyMapper)
    //dataStream.print()

    //----------filter----------
    /*val filterStream1: DataStream[SensorReading] = dataStream.filter(data => {
        data.id.startsWith("sensor_1")
    })
    filterStream1.print()*/
    //以上等效：
    /*val filterStream2: DataStream[SensorReading] = dataStream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = {
        t.id.startsWith("sensor_1")
      }
    })
    filterStream2.print()*/

    /*val filterStream3: DataStream[SensorReading] = dataStream.filter(new MyRichFilter)
    val filterStream4: DataStream[SensorReading] = dataStream.filter(new MyFilter)*/

    //----------keyBy----------
    val keyedStream: KeyedStream[SensorReading, String] = dataStream.keyBy(x => x.id)
    //val aggStream: DataStream[SensorReading] = keyedStream.max("temperature")
    //aggStream.print()
    //求temperature的最大值，timestamp不会变

    /*val aggStream2: DataStream[SensorReading] = keyedStream.maxBy("temperature")
    aggStream2.print()
    //求temperature的最大值，timestamp会改变*/

    val aggStream3: DataStream[SensorReading] = keyedStream.reduce((curData, newData) => {
      SensorReading(curData.id, curData.timestamp, newData.temperature.max(curData.temperature))
    })
    aggStream3.print()

    env.execute("transform demo")

  }
}

class MyFilter extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean =
    t.id.startsWith("sensor_1")
}

class MyRichFilter extends RichFilterFunction[SensorReading]{
  override def open(parameters: Configuration): Unit = {

  }

  override def filter(t: SensorReading): Boolean =
    t.id.startsWith("sensor_1")

  override def close(): Unit = {

  }
}

class MyReducer extends ReduceFunction[SensorReading]{
  override def reduce(curData: SensorReading, newData: SensorReading): SensorReading = {
    SensorReading(curData.id, curData.timestamp, newData.temperature.max(curData.temperature))
  }
}

class MyRichReducer extends RichReduceFunction[SensorReading]{
  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def reduce(curData: SensorReading, newData: SensorReading): SensorReading = {
    SensorReading(curData.id, curData.timestamp, newData.temperature.max(curData.temperature))
  }

  override def close(): Unit = {

  }
}

class MyMapper extends MapFunction[String,SensorReading]{   //输入String，输出SensorReading
  override def map(value: String): SensorReading ={
    val strings = value.split(",")
    SensorReading(strings(0),strings(1).toLong,strings(2).toDouble)
  }
}

class MyRichMapper extends RichMapFunction[String,SensorReading]{
  override def map(in: String): SensorReading =
    SensorReading("",1,1)

  override def open(parameters: Configuration): Unit = super.open(parameters)
}
