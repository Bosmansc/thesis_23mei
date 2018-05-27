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


//    val test = FeatureCalculation.calculation(stream)


// stream to table

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)


    // de volgende fin measures nog in klassen steken


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

    CCI_2_table.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)]



// Stochastic Oscillator

    val stoch = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, 100*((( lastPrice - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/" +
      "                           ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ))) as K " +
      "                           FROM stockTable ")

    val stoch_table = stoch.toAppendStream[( Timestamp,String, Double,Double)]

    tableEnv.registerDataStream("stoch_table_2", stoch_table, 'stockTime, 'stockName, 'lastPrice,'K, 'UserActionTime.proctime )

    val stoch_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName,lastPrice, K, ( AVG(K)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) ) as D "  +
      "                                    FROM stoch_table_2" +
      "                                    WHERE stockName = 'AAPL UW Equity'")

    stoch_table_2.toAppendStream[(Timestamp, String, Double, Double, Double)]



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
      "                                 ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ) as RS," +

      "                                  100 - 100/( 1 + ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )) as RSI"  +

    "                                    FROM rsi_table_1" +
    "                                    WHERE stockName = 'AAPL UW Equity'")


    val rsi_table_2 = rsi_table_1.toAppendStream[(Timestamp, String, Double, Double, Double,  Double,Double)]



// Money Flow Indicator

    val mfi = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, (high + low + lastPrice)/3 as typicalPrice, ((high + low + lastPrice)/3)*volume as moneyFlow" +
      "                         FROM stockTable ")



    val mfi_tbl = mfi.toAppendStream[(Timestamp, String, Double,Double, Double)]

    tableEnv.registerDataStream("mfi_table_1", mfi_tbl, 'stockTime, 'stockName, 'lastPrice, 'typicalPrice, 'moneyFlow, 'UserActionTime.proctime )

    val mfi_tbl_1 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +

      "                               CASE WHEN ROUND(2*moneyFlow - (SUM(moneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) > 0 THEN " +
      "                               ROUND( 2*moneyFlow - (SUM(moneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) ,6) ELSE 0 END as posMoneyFlow, " +

      "                               CASE WHEN ROUND(2*moneyFlow - (SUM(moneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) < 0 THEN " +
      "                               -ROUND( 2*moneyFlow - (SUM(moneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) ,6) ELSE 0 END as negMoneyFlow" +

      "                               FROM mfi_table_1 ")

    val mfi_tbl_2 = mfi_tbl_1.toAppendStream[(Timestamp, String, Double, Double, Double)]

    tableEnv.registerDataStream("mfi_table_2", mfi_tbl_2, 'stockTime, 'stockName, 'lastPrice, 'posMoneyFlow, 'negMoneyFlow, 'UserActionTime.proctime )

    val mfi_tbl_3 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +

      "                               ( AVG(posMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/" +
      "                               ( AVG(negMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ) as moneyRatio," +

      "                               100 - 100/( 1 + ( (AVG(posMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/" +
      "                               ( AVG(negMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ) ) )as moneyFlowIndex" +


      "                               FROM mfi_table_2 " +
      "                               WHERE stockName = 'ABT UN Equity'")

    val mfi_stream = mfi_tbl_3.toAppendStream[(Timestamp, String, Double, Double, Double)]




// Chaikin Accumulation/Distribution

   //   [(Close  -  Low) - (High - Close)] /(High - Low) = Money Flow Multiplier


    val chai = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, volume, ( (lastPrice - low) - (high - lastPrice) )/( high - low) as moneyFlowMultiplier," +
      "                           CASE WHEN (high-low) > 0 THEN ( ( (lastPrice - low) - (high - lastPrice) )/( high - low) ) * volume ELSE 0 END as moneyFlowVolume" +
      "                            FROM stockTable ")

    val chai_tbl = chai.toAppendStream[(Timestamp, String, Double,Double, Double, Double)]



    tableEnv.registerDataStream("chai_tbl_1", chai_tbl, 'stockTime, 'stockName, 'lastPrice, 'volume, 'moneyFlowMultiplier, 'moneyFlowVolume, 'UserActionTime.proctime )

    val chai_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, moneyFlowMultiplier, moneyFlowVolume, " +

      "                                  ( SUM(moneyFlowVolume) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) )/" +
      "                                  ( SUM(volume) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) ) as chaikinMoneyFlow" +
      "                                   FROM chai_tbl_1 " +
      "                                   WHERE stockName = 'ABT UN Equity'")

    val chai_stream = chai_table_2.toAppendStream[(Timestamp, String, Double,Double, Double, Double)]



// William' %R

    // %R = (Highest High - Close)/(Highest High - Lowest Low) * -100

    val wilR = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +
      "                           -100 * ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) - lastPrice )/" +
      "                           ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) ) as williamsR " +
      "                           FROM stockTable" +
      "                           WHERE stockName = 'ABBV UN Equity' ")

    val wilR_stream = wilR.toAppendStream[(Timestamp, String, Double,Double)]

    wilR_stream.print()




    env.execute()
  }
}
