package thesis

import java.sql.Timestamp
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

    val stream: DataStream[StockQuotes] = env.addSource(new FlinkKafkaConsumer08[String]("stock", new SimpleStringSchema(), properties))
      .map(StockQuotes.fromString(_))


    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'open, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

// financial measure SMA:

    val SMA10 = tableEnv.sqlQuery("SELECT UserActionTime, stockName , ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY stockTime" +
      "                           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW),4) as SMA10" +
      "                           FROM stockTable" +
      "                           ")

    val SMA101 = tableEnv.sqlQuery("SELECT  CAST(stockTime as TIMESTAMP), stockTime"  +
      "                           FROM stockTable" +
      "                           ")

// Financial measure Bollinger Bands,

    val BB = tableEnv.sqlQuery("SELECT stockName , ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),3) as BB_middleBand," +

                                  "ROUND( AVG(lastPrice)" +
      "                           OVER ( PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),3) + " +
      "                           2*(STDDEV_SAMP(lastPrice) OVER (PARTITION BY stockName " +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)) as BB_upperBound," +

      "                           ROUND( AVG(lastPrice)" +
      "                           OVER ( PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),3) - " +
      "                           2*(STDDEV_SAMP(lastPrice) OVER (PARTITION BY stockName " +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)) as BB_lowerBound" +

      "                           FROM stockTable" +
      "                           ")

// Commodity Channel Index, problemen met MAD!!


    val CCI_1 = tableEnv.sqlQuery("SELECT UserActionTime, " +
      "                           66.6666* ( (high + low + lastPrice)/3 -  AVG(lastPrice) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) ), " +

      "                          ABS( (high + low + lastPrice)/3 - AVG(high + low + lastPrice) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) ) as deviation" +
      "                           " +

      "                          from stockTable")





// merging 2 sql-tables

    tableEnv.registerTable("SMA10", SMA10)
    tableEnv.registerTable("CCI_1", CCI_1)

    val merg = tableEnv.sqlQuery("SELECT *" +
      "                           FROM SMA10 o,  CCI_1 s" +
    "                             WHERE o.UserActionTime = s.UserActionTime")

    // Transform table to stream and print

    SMA101.toRetractStream[(Timestamp, String)].print()



    env.execute()
  }
}
