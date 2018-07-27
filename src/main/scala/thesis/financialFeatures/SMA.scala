package thesis.financialFeatures

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes


case class SMAtypes(stockTime: Timestamp, stockName: String, lastPrice: Double, SMA10: Double, SMA100:Double, SMA_signal: Int, SMA_direction: Int)

object SMA {

  def calculateSMA(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[SMAtypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime, 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    val SMA10 = tableEnv.sqlQuery("SELECT stockTime, stockName , lastPrice, ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW),4) as SMA10" +
      "                           FROM stockTable" +
      "                           ")

    val SMA10_table = SMA10.toAppendStream[(Timestamp, String, Double, Double)]

    tableEnv.registerDataStream("SMA10", SMA10_table, 'stockTime, 'stockName, 'lastPrice, 'SMA10, 'UserActionTime.proctime)

    val SMA100 = tableEnv.sqlQuery("SELECT stockTime, stockName ,  ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 100 PRECEDING AND CURRENT ROW),4) as SMA100" +
      "                           FROM stockTable" +
      "                           ")

    val SMA100_table = SMA100.toAppendStream[(Timestamp, String, Double)]

    tableEnv.registerDataStream("SMA100", SMA100_table, 'stockTime, 'stockName, 'SMA100, 'UserActionTime.proctime)

    val SMA10_lag = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, SMA10.lastPrice,  SUM(SMA10.SMA10) OVER (PARTITION BY SMA10.stockName ORDER BY SMA10.UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - SMA10.SMA10 as SMA10lag" +
      "                             " +
      "                             FROM SMA10 ")


    val SMA10_lag_table = SMA10_lag.toAppendStream[(Timestamp, String, Double, Double)]

    tableEnv.registerDataStream("SMA10_lag", SMA10_lag_table, 'stockTime, 'stockName, 'lastPrice, 'SMA10lag, 'UserActionTime.proctime)

    val SMA100_lag = tableEnv.sqlQuery("SELECT stockTime,stockName ," +
      "                             SUM(SMA100.SMA100) OVER (PARTITION BY SMA100.stockName ORDER BY SMA100.UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - SMA100.SMA100 as SMA100lag" +
      "                             FROM  SMA100 ")


    val SMA100_lag_table = SMA100_lag.toAppendStream[(Timestamp, String, Double)]

    tableEnv.registerDataStream("SMA100_lag", SMA100_lag_table, 'stockTime, 'stockName, 'SMA100lag, 'UserActionTime.proctime)


    // SMA buy/sell/hold-signal: 1 = BUY, 2 = SELL, 0 = HOLD

    /*
  Another strategy is to apply two moving averages to a chart: one longer and one shorter.
  When the shorter-term MA crosses above the longer-term MA, it's a buy signal,

   */

    /*
  Direction Increase if MA (t - 1) < close (t)
  Decrease if MA (t - 1) > close (t)
   */


    val SMA_signal_big = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, SMA10.lastPrice, SMA10.SMA10, SMA100.SMA100, SMA10_lag.SMA10lag, SMA100_lag.SMA100lag," +
      "                                 CASE WHEN SMA10.SMA10 <= SMA100.SMA100 AND SMA10_lag.SMA10lag > SMA100_lag.SMA100lag THEN 2" +
      "                                 WHEN SMA10.SMA10 >= SMA100.SMA100 AND SMA10_lag.SMA10lag < SMA100_lag.SMA100lag THEN 1 ELSE 0 END as SMA_signal," +
      "" +
      "                                 CASE WHEN SMA10_lag.SMA10lag < SMA10.lastPrice THEN 1 " +
      "                                 WHEN SMA10_lag.SMA10lag >= SMA10.lastPrice THEN -1 ELSE 0 END AS direction" +

      "                                 FROM SMA10, SMA100, SMA10_lag, SMA100_lag " +
      "                                 WHERE SMA10.stockTime = SMA100.stockTime AND SMA10.stockName = SMA100.stockName " +
      "                                 AND SMA10_lag.stockTime = SMA10.stockTime AND   SMA10.stockName =  SMA10_lag.stockName" +
      "                                 AND SMA100_lag.stockTime = SMA10.stockTime AND   SMA10.stockName =  SMA100_lag.stockName")

    val SMA_signal_big_table = SMA_signal_big.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Int, Int)]

    tableEnv.registerDataStream("SMA_signal_big_table", SMA_signal_big_table, 'stockTime, 'stockName, 'lastPrice, 'SMA10, 'SMA100, 'SMA10lag, 'SMA100lag, 'SMA_signal, 'SMA_direction, 'UserActionTime.proctime)

    val SMA_signal_table = tableEnv.sqlQuery("SELECT stockTime,stockName ,lastPrice, SMA10, SMA100, SMA_signal, SMA_direction" +
      "                                       " +
      "                                       FROM  SMA_signal_big_table ")

    val SMA_signal = SMA_signal_table.toAppendStream[(SMAtypes)]

    SMA_signal_table.toAppendStream[(SMAtypes)]
  }


}
