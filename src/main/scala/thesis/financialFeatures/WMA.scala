package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes


case class WMATypes(stockTime: Timestamp, stockName: String, lastPrice: Double, WMA: Double)

object WMA {

  def calculateWMA(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[WMATypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime, 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)



    // weighted moving average

    val lag1 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +
      "                          ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - lastPrice,4) as lastPriceLag1" +
      "                          FROM stockTable")

    val lag1_stream = lag1.toAppendStream[(Timestamp, String, Double, Double)]

    tableEnv.registerDataStream("lag1_table", lag1_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'UserActionTime.proctime)

    val lag2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, " +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1,4) as lastPriceLag2" +
      "                           FROM lag1_table" +
      "                            ")

    val lag2_stream = lag2.toAppendStream[(Timestamp, String, Double, Double, Double)]

    tableEnv.registerDataStream("lag2_table", lag2_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'UserActionTime.proctime)

    val lag3 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2,4) as lastPriceLag3" +
      "                           FROM lag2_table" +
      "                            ")

    val lag3_stream = lag3.toAppendStream[(Timestamp, String, Double, Double, Double, Double)]

    tableEnv.registerDataStream("lag3_table", lag3_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'UserActionTime.proctime)

    val lag4 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2,lastPriceLag3," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3,4) as lastPriceLag4" +
      "                           FROM lag3_table" +
      "                            ")

    val lag4_stream = lag4.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("lag4_table", lag4_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'UserActionTime.proctime)

    val lag5 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2,lastPriceLag3,lastPriceLag4," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3 - lastPriceLag4,4) as lastPriceLag5" +
      "                           FROM lag4_table" +
      "                            ")

    val lag5_stream = lag5.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("lag5_table", lag5_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'lastPriceLag5, 'UserActionTime.proctime)

    val lag6 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2,lastPriceLag3,lastPriceLag4,lastPriceLag5," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3 - lastPriceLag4 - lastPriceLag5 ,4) as lastPriceLag6" +
      "                           FROM lag5_table" +
      "                            ")

    val lag6_stream = lag6.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("lag6_table", lag6_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'lastPriceLag5, 'lastPriceLag6, 'UserActionTime.proctime)

    val lag7 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2,lastPriceLag3,lastPriceLag4,lastPriceLag5, lastPriceLag6," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3 - lastPriceLag4 - lastPriceLag5 - lastPriceLag6  ,4) as lastPriceLag7" +
      "                           FROM lag6_table" +
      "                            ")

    val lag7_stream = lag7.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("lag7_table", lag7_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'lastPriceLag5, 'lastPriceLag6, 'lastPriceLag7, 'UserActionTime.proctime)

    val lag8 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2,lastPriceLag3,lastPriceLag4,lastPriceLag5, lastPriceLag6, lastPriceLag7," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3 - lastPriceLag4 - lastPriceLag5 - lastPriceLag6  -lastPriceLag7 ,4) as lastPriceLag8" +
      "                           FROM lag7_table" +
      "                            ")

    val lag8_stream = lag8.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("lag8_table", lag8_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'lastPriceLag5, 'lastPriceLag6, 'lastPriceLag7, 'lastPriceLag8, 'UserActionTime.proctime)

    val lag9 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2, lastPriceLag3, lastPriceLag4, lastPriceLag5, lastPriceLag6, lastPriceLag7, lastPriceLag8," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3 - lastPriceLag4 - lastPriceLag5 - lastPriceLag6  -lastPriceLag7 - lastPriceLag8,4) as lastPriceLag9" +
      "                           FROM lag8_table" +
      "                            ")

    val lag9_stream = lag9.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("lag9_table", lag9_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'lastPriceLag5, 'lastPriceLag6, 'lastPriceLag7, 'lastPriceLag8, 'lastPriceLag9, 'UserActionTime.proctime)

    val lag10 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2,lastPriceLag3,lastPriceLag4,lastPriceLag5, lastPriceLag6,lastPriceLag7,lastPriceLag8, lastPriceLag9," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3 - lastPriceLag4 - lastPriceLag5 - lastPriceLag6  -lastPriceLag7 - lastPriceLag8 - lastPriceLag9 ,4) as lastPriceLag10" +
      "                           FROM lag9_table")

    val lag10_stream = lag10.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)]


    tableEnv.registerDataStream("lag10_table", lag10_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'lastPriceLag5, 'lastPriceLag6, 'lastPriceLag7, 'lastPriceLag8, 'lastPriceLag9, 'lastPriceLag10, 'UserActionTime.proctime)

    val lag11 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, lastPriceLag1, lastPriceLag2,lastPriceLag3,lastPriceLag4,lastPriceLag5, lastPriceLag6,lastPriceLag7,lastPriceLag8, lastPriceLag9, lastPriceLag10," +
      "                           ROUND(SUM(lastPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) - lastPrice - lastPriceLag1 - lastPriceLag2 - lastPriceLag3 - lastPriceLag4 - lastPriceLag5 - lastPriceLag6  -lastPriceLag7 - lastPriceLag8 - lastPriceLag9 - lastPriceLag10,4) as lastPriceLag11" +
      "                           FROM lag10_table" +
      "                            ")

    val lag11_stream = lag11.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("wma_table", lag11_stream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag1, 'lastPriceLag2, 'lastPriceLag3, 'lastPriceLag4, 'lastPriceLag5, 'lastPriceLag6, 'lastPriceLag7, 'lastPriceLag8, 'lastPriceLag9, 'lastPriceLag10, 'lastPriceLag11, 'UserActionTime.proctime)

    val wma_table = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, ( 10*lastPrice + 9* lastPriceLag1 + 8* lastPriceLag2 + 7*lastPriceLag3 + 6*lastPriceLag4 + 5*lastPriceLag5 + 4* lastPriceLag6 + 3*lastPriceLag7 + 2* lastPriceLag8 + lastPriceLag9 )/55 as WMA10" +
      "                                FROM wma_table")

    val wma_stream = wma_table.toAppendStream[(Timestamp, String, Double, Double)]
    wma_table.toAppendStream[(WMATypes)]

    /*
    Buy if WMA10 (t - 1) <= WMA100 (t - 1) and WMA10 (t) > WMA100 (t)
    Sell if WMA10 (t - 1) >= WMA100 (t - 1) and WMA10 (t) < WMA100 (t)
    Hold otherwise
    1 = BUY, 2 = SELL, 0 = HOLD
     */


  }


}
