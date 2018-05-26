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

// stream to table

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)


// financial measure SMA:

    val SMA10 = tableEnv.sqlQuery("SELECT  stockName , stockTime, ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW),4) as SMA10" +
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


// Commodity Channel Index, gewerkt met table -> stream -> table -> stream, mag dit?


    val CCI_1 = tableEnv.sqlQuery("SELECT stockTime, stockName, (high + low + lastPrice)/3 as typicalPrice," +
      "                            (high + low + lastPrice)/3 -  AVG((high + low + lastPrice)/3) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as numerator_CCI," +
      "                            " +

      "                          ABS( (high + low + lastPrice)/3 - AVG((high + low + lastPrice)/3) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) ) as deviation" +
      "                           " +

      "                          FROM stockTable")

    val CCI_1_table = CCI_1.toAppendStream[( Timestamp,String, Double,Double, Double)]

    tableEnv.registerDataStream("CCI_1_table", CCI_1_table, 'stockTime, 'stockName, 'typicalPrice, 'numerator_CCI, 'deviation, 'UserActionTime.proctime )

    val CCI_2_table = tableEnv.sqlQuery("SELECT stockTime, stockName,typicalPrice,  numerator_CCI,deviation,   AVG(deviation)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as MAD, " +
      "                                 66.6666666667* ( numerator_CCI/(AVG(deviation)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) ) as CCI"  +
      "                                 FROM CCI_1_table" +
      "                                 WHERE stockName = 'AAPL UW Equity'" +
      "                           ")

    CCI_2_table.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)].print()



// Stochastic Oscillator
// %K kunnen berekenen, nu 3 day moving average nodig over K -> via join mss mogelijk

    val stoch = tableEnv.sqlQuery("SELECT (( lastPrice - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) )/" +
      "                           ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) )) as K " +
      "                           FROM stockTable ")

// Relative Strength Index

    

// Money Flow Indicator




// merging 2 sql-tables

    tableEnv.registerTable("SMA10", SMA10)
   // tableEnv.registerTable("SMA101", SMA101)
    tableEnv.registerTable("CCI_1", CCI_1)




/*
    Table result = SMA10.join(SMA101).where("")

      .where("a = d && ltime >= rtime - 5.minutes && ltime < rtime + 10.minutes")
      .select("a, b, e, ltime");
*/
/*
    val merg = tableEnv.sqlQuery("SELECT *" +
      "                           FROM SMA10 s,  CCI_1 t" +
    "                             WHERE s.stockTime = t.stockTime")
*/

// Transform table to stream and print


 //   SMA10.toRetractStream[(String, Double)].print()
    val sma_tbl = SMA10.toRetractStream[(String,Timestamp, Double)]
    val sma_tbl2 = SMA10.toAppendStream[(String,Timestamp, Double)]

    tableEnv.registerDataStream("sma_tbl2", sma_tbl2, 'stockName, 'stockTime, 'SMA10 , 'UserActionTime.proctime)

    /*
    val SMA10_tbl = tableEnv.sqlQuery("SELECT stockName, stockTime,  SMA10 ROUND(AVG(SMA10)  OVER ( PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),4) as SMA20"  +
      "                           FROM sma_tbl2" +
      "                           ")
*/

 // SMA10_tbl.toRetractStream[(String, Double)].print()
 // SMA10_tbl.toAppendStream[(String,Timestamp, Double)].print()



    env.execute()
  }
}
