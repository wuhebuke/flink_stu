package cn.entity.sink

import cn.entity.apiTest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}


object HBaseSinkTest {
  def main(args: Array[String]): Unit = {
    //创建flink环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置程序运行并行度为1
    env.setParallelism(1)

    //添加flink数据源
    val stream1: DataStream[String] = env.socketTextStream("single03", 7777)

    //通过map对字符串流进行结构转换
    val dataStream: DataStream[SensorReading] = stream1.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    })

    //将dataStream流中的数据写入hbase，自定义SinkFunction
    dataStream.addSink(new MyHBaseSinkFunc())

    env.execute("hbase sink demo")
  }
}

class MyHBaseSinkFunc() extends RichSinkFunction[SensorReading]{
  var connection: Connection = _
  var mutator:BufferedMutator= _

  override def open(parameters: Configuration): Unit = {
    val config: conf.Configuration = HBaseConfiguration.create()
    config.set(HConstants.HBASE_DIR,"hdfs://single03:9000/hbase")
    config.set(HConstants.ZOOKEEPER_QUORUM,"single03")
    config.set(HConstants.CLIENT_PORT_STR,"2181")
    connection = ConnectionFactory.createConnection(config)

    val params = new BufferedMutatorParams(TableName.valueOf("events_db:hbsink"))
    params.writeBufferSize(10*1024*1024)
    params.setWriteBufferPeriodicFlushTimeoutMs(5*1000L)
    mutator = connection.getBufferedMutator(params)
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
    val put = new Put(Bytes.toBytes((value.id + value.temperature + value.timestamp).hashCode))
    put.addColumn("sensor".getBytes(),"id".getBytes,value.id.getBytes())
    put.addColumn("sensor".getBytes(),"temperature".getBytes,value.temperature.toString.getBytes())
    put.addColumn("sensor".getBytes(),"timestamp".getBytes,value.timestamp.toString.getBytes())

    mutator.mutate(put)
    mutator.flush()

  }

  override def close(): Unit = {
    mutator.close()
    connection.close()
  }
}
