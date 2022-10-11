package cn.entity.apiTest

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.{lang, util}

object StateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[String] = env.socketTextStream("single03", 7777)

    /*val dataStream: DataStream[String] =
      stream1.map(data => {
        val strings = data.split(",")
        SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
      }).keyBy(_.id).map(new MyMapStateFunc)
    dataStream.print("info")*/

    //需求：温度跳变幅度大于等于10，报警
    /*val alarmStream: DataStream[(String, Double, Double)] = stream1.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }).keyBy(_.id).flatMap(new MyAlarmFunc(10))
    alarmStream.print("info")*/

    val listStateStream: DataStream[String] = stream1.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }).keyBy(_.id).map(new MyMapStateFunc)
    listStateStream.print("info")

    env.execute("state demo")

  }
}

//温差大报警
//如何解决第一次传入温度即有温差（由于默认preTemp为0，即第一次无论传入任何温度，都存在大的温度差）
class MyAlarmFunc(diff:Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  lazy val preTempState:ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("preTempState",classOf[Double]))
  lazy val firstTagState:ValueState[Boolean]=
    getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firstTag",classOf[Boolean]))

  override def flatMap(in: SensorReading, collector: Collector[(String,Double,Double)]): Unit = {
    val preTemp: Double = preTempState.value()
    val bool: Boolean = firstTagState.value()
    if (!bool){
      firstTagState.update(true)
      preTempState.update(in.temperature)
    }else{
      //温差是否大于diff
      val sub: Double = (preTemp - in.temperature).abs
      if(sub>=diff){
        collector.collect(in.id,in.temperature,preTemp)
      }
    }
    preTempState.update(in.temperature)
  }
}

//状态编程
class MyMapStateFunc extends RichMapFunction[SensorReading,String]{
  var valueState:ValueState[Double] = _
  lazy val listState:ListState[Double] =
    getRuntimeContext.getListState(new ListStateDescriptor[Double]("listState",classOf[Double]))
  lazy val mapState:MapState[String,Double]=
    getRuntimeContext.getMapState(new MapStateDescriptor[String,Double]("mapState",classOf[String],classOf[Double]))
  lazy val reduceState:ReducingState[SensorReading]=
    getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduceState",new MyReducer,classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    //初始化
    valueState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("valueState",classOf[Double])
    )
  }

  override def map(in: SensorReading): String = {
    val reading: SensorReading = reduceState.get()
    if(reading!=null) {
      println(reading)
    }

    reduceState.add(in)
    ""


    /*if (!mapState.contains(in.id))
      mapState.put(in.id,in.temperature)
      else{
      val num: Double = mapState.get(in.id)
      println(num)
      mapState.put(in.id,in.temperature)
    }
    ""*/

    /*listState.add(in.temperature)
    val temps: lang.Iterable[Double] = listState.get()
    val iterator: util.Iterator[Double] = temps.iterator()
    var sum:Double=0.0
    while (iterator.hasNext){
      val d: Double = iterator.next()
      println(d)
      sum+=d
    }
    sum.toString*/

    /*val preValue: Double = valueState.value()
    println(preValue,in)
    if (preValue != null) {
      valueState.update(in.temperature)
      if (preValue>in.temperature)
        "温度降低"
      else if (preValue==in.temperature)
        "温度不变"
      else
        "温度升高"
    }else
      "none"*/

  }

  override def close(): Unit = {

  }
}

