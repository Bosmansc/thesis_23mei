package thesis


import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._


object Table {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "stock")

    val stream: DataStream[StockFeatures] = env.addSource(new FlinkKafkaConsumer08[String]("stock", new SimpleStringSchema(), properties))
      .map(StockFeatures.fromString(_))


    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime, 'open, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)


    val SMA10 = tableEnv.sqlQuery("SELECT stockName , ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW),4) as SMA10" +
      "                           FROM stockTable" +
      "                           ")


    SMA10.toRetractStream[(String, Double)].print()


    env.execute()
  }
}
