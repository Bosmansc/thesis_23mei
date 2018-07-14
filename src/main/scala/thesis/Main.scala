package thesis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sinks.CsvTableSink



object Main {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "stock6")

    val stream: DataStream[StockQuotes] = env.addSource(new FlinkKafkaConsumer08[String]("stock6", new SimpleStringSchema(), properties))
      .map(StockQuotes.fromString(_))


    // from which value a stock is considered to rise/fall: (recommended values: 0.01, 0.1, 0.2, 0.5, 0.75)
    val threshold = 0.1

    val test = FeatureCalculation.calculation(stream,threshold)

    // convert stream to dataSet (stream to batch to make predictions)
    val table1: Table = tableEnv.fromDataStream(test)


  //  test.print()

    // nog loop maken die na aantal sec stopt, https://stackoverflow.com/questions/18358212/scala-looping-for-certain-duration werkt niet

    // work with if: if return is to low: then write to sink
      table1.writeToSink(
        new CsvTableSink(
          "C:\\Users\\ceder\\Flink\\batch2",                             // output path
          fieldDelim = ",",                 // optional: delimit files by '|'
          numFiles = 1,                     // optional: write to a single file
          writeMode = WriteMode.OVERWRITE)) // optional: override existing files*/



    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    // Relative Strength Index

    val rsi = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +
      "                          ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - lastPrice,4) as lastPriceLag," +
      "                          ROUND(2* lastPrice - (SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) as difference," +

      "                          CASE WHEN  ROUND(2* lastPrice - (SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) > 0 THEN " +
      "                          ROUND(2* lastPrice - (SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) ELSE 0 END AS posDifference, " +

      "                           CASE WHEN  ROUND(2* lastPrice - (SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) < 0 THEN " +
      "                           -ROUND(2* lastPrice - (SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) ELSE 0 END AS negDifference" +

      "                           FROM stockTable" +
      "                           ")

    val rsi_table = rsi.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("rsi_table_1", rsi_table, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag, 'difference, 'posDifference, 'negDifference, 'UserActionTime.proctime )

    val rsi_table_1 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, " +

      "                                 CASE WHEN AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) < 0 OR  AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) > 0" +
      "                                 THEN ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ) ELSE 0  END as RS," +

      "                                  100 - 100/( 1 + ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )) as RSI"  +

      "                                   FROM rsi_table_1")

    val RSIsignal_table = rsi_table_1.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("RSIsignal_table", RSIsignal_table, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'RS , 'RSI, 'UserActionTime.proctime )

    val rsi_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, " +
      "                                 RS, " +
      "                                SUM(RS) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - RS as RSlag, " +
      "                                 SUM(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - posDifference as posDifferencelag," +
      "                                 SUM(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - negDifference as negDifferencelag, RSI" +

      "                                   FROM RSIsignal_table" +
      "                                   WHERE stockName ='AAPL UW Equity'"
                          )

    val RSIsignal_table2 = rsi_table_2.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("RSIsignal_table3", RSIsignal_table2, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'RS , 'RSlag,'posDifferencelag,'negDifferencelag,  'RSI, 'UserActionTime.proctime )

    val rsi_table_3 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, " +
      "                                 " +
      "                                 " +
      "                                   CASE WHEN stockTime BETWEEN TIMESTAMP '2015-04-27 15:32:00.0' AND TIMESTAMP '2015-04-27 15:44:00.0' AND posDifference < 10 " +
      "                                   THEN AVG(posDifference)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) ELSE " +
      "                                   (posGainLag*13 + posDifference)/14 END as posGain,  " +

      "                                   CASE WHEN stockTime BETWEEN TIMESTAMP '2015-04-27 15:32:00.0' AND TIMESTAMP '2015-04-27 15:44:00.0' THEN " +
      "                                   AVG(negDifference)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) ELSE 0 END as negGain " +
      "                                   " +

      "                                   FROM RSIsignal_table3" +
      "                                   WHERE stockName ='AAPL UW Equity'"
    )

    val RSIsignal_table3 = rsi_table_3.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)]


    tableEnv.registerDataStream("RSIsignal_table4", RSIsignal_table3, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'posGain, 'negGain, 'UserActionTime.proctime )

    val rsi_table4 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, posGain, negGain, " +
      "                                 SUM(posGain)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - posGain as posGainLag," +
      "                                 SUM(negGain)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - negGain as negGainLag" +
      "                                  " +


      "                                   FROM RSIsignal_table4" +
      "                                   WHERE stockName ='AAPL UW Equity'")

    val RSIsignal_4 = rsi_table4.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("RSIsignal_table5", RSIsignal_4, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'posGain, 'negGain, 'posGainLag, 'negGainLag, 'UserActionTime.proctime )

    val rsi_table5 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, posGain, negGain, " +
      "                                  " +
      "                                  negGainLag," +
      "                                  " +
      "" +
      "                                 CASE WHEN stockTime BETWEEN TIMESTAMP '2015-04-27 15:32:00.0' AND TIMESTAMP '2015-04-27 15:44:00.0' AND posDifference < 10 " +
      "                                 THEN posGain/negGain ELSE 0 END as RSstart, " +

      "                                 CASE WHEN stockTime NOT BETWEEN TIMESTAMP '2015-04-27 15:32:00.0' AND TIMESTAMP '2015-04-27 15:44:00.0' THEN " +
      "                                 (posGainLag*13 + posDifference)/14 ELSE 0 END as posGain2" +


      "                                   FROM RSIsignal_table5" +
      "                                   WHERE stockName ='AAPL UW Equity'")

    val RSIsignal_5 = rsi_table5.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double, Double)]




    RSIsignal_5.print()



    env.execute()
  }
}
