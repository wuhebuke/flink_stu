package cn.entity.apiTest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

//侧输出流 (分流)
object SideOutPutTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataList = List(
        "sensor_1,1665103467,35.8",
        "sensor_4,1665103470,18.6",
        "sensor_7,1665103473,42.3",
        "sensor_5,1665103473,42.3",
        "sensor_9,1665103475,25.5",
        "sensor_1,1665103456,35.5",
        "sensor_1,1665103464,20.9",
        "sensor_1,1665103458,16.6"
    )

    val dataList2 = List(
      "sensor_1,1665103467,35.8",
      "sensor_4,1665103470,18.6",
      "sensor_7,1665103473,42.3",
      "sensor_5,1665103473,42.3"
    )

    val inputStream2: DataStream[String] = env.fromCollection(dataList2)
    val inputStream: DataStream[String] = env.fromCollection(dataList)

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    })
    val dataStream2: DataStream[SensorReading] = inputStream2.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    })


    //分流: 30分以上，30分以下
    /*val highTempStream: DataStream[SensorReading] = dataStream.process(new SplitTempProcess)
    highTempStream.print("high")
    val lowTempStream: DataStream[SensorReading] = highTempStream.getSideOutput(new OutputTag[SensorReading]("low temp"))
    lowTempStream.print("low")*/

    /*val normal: DataStream[SensorReading] = dataStream.process(new SplitTempProcess1)
    normal.print("normal")
    val low = normal.getSideOutput(new OutputTag[SensorReading]("low"))
    low.print("low")*/

    /*val normalTemp: DataStream[SensorReading] = dataStream.process(new SplitTempProcess2(40, 30))
    //normal.print("normal")
    val lowTemp: DataStream[SensorReading] = normalTemp.getSideOutput(new OutputTag[SensorReading]("low"))
    //low.print("low")
    val highTemp: DataStream[SensorReading] = normalTemp.getSideOutput(new OutputTag[SensorReading]("high"))
    //high.print("high")


    //合流
    val unionStream: DataStream[SensorReading] = highTemp.union(lowTemp)
    unionStream.print("b")*/

    //union必须连接两个相同结构的数据
    // val unionStream2: DataStream[SensorReading] = dataStream.union(dataStream2)
    // unionStream2.print("a")

    //相同结构数据的connect
    //val connectStream: ConnectedStreams[SensorReading, SensorReading] = dataStream2.connect(dataStream)

    //不同结构数据的connect
    //人为将dataStream流修改成 二元组流 (String, Double)，再将二元组流和SensorReading流进行connect
    //val connectStream2: ConnectedStreams[SensorReading, (String, Double)] = dataStream2.connect(dataStream.map(x => (x.id, x.temperature)))
    val dataStreamMap: DataStream[(String, Double)] = dataStream.map(x => (x.id, x.temperature))
    val connectStream3: ConnectedStreams[SensorReading, (String, Double)] = dataStream2.connect(dataStreamMap)
    val dataStreamConnect: DataStream[SensorReading] = connectStream3.map(
      data1 => {
        data1
      },
      data2 => {
        SensorReading(data2._1, 0L, data2._2)
      }
    )
    dataStreamConnect.print("dataStreamConnect")

    env.execute("sideOutPut demo")

  }

}

class SplitTempProcess() extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(value: SensorReading,
                              context: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if (value.temperature>30){
      out.collect(value)
    }else{
      context.output(new OutputTag[SensorReading]("low temp"),value)     //侧输出流标签
    }
  }
}

class SplitTempProcess1() extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature<30){
      context.output(new OutputTag[SensorReading]("low"),i)
    }else if(i.temperature>40){
      context.output(new OutputTag[SensorReading]("high"),i)
    }else{
      collector.collect(i)
    }
  }
}

class SplitTempProcess2(high:Double,low:Double) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if(i.temperature>high){
      context.output(new OutputTag[SensorReading]("high"),i)
    }else if(i.temperature<low){
      context.output(new OutputTag[SensorReading]("low"),i)
    }else{
      collector.collect(i)
    }
  }
}