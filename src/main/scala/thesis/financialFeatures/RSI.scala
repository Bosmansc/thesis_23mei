package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class RSITypes(stockTime: Timestamp, stockName: String,  lastPrice:Double, RSI: Double, RSI_signal: Int, RSI_direction:Int )

object RSI {

  def calculateRSI(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[RSITypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
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

      "                                 ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) ) as RS," +

      "                                  100 - 100/( 1 + ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) )) as RSI"  +

      "                                   FROM rsi_table_1")

    val RSIsignal_table = rsi_table_1.toAppendStream[( Timestamp, String, Double, Double, Double, Double, Double)]


    tableEnv.registerDataStream("RSIsignal_table", RSIsignal_table, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'RS , 'RSI, 'UserActionTime.proctime )

    val rsi_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, " +
      "                                 RS, " +
      "                                  CASE WHEN RS > 100 THEN 1 ELSE RS END AS RS1, RSI" +

      "                                   FROM RSIsignal_table")

    val RSIsignal_table2 = rsi_table_2.toAppendStream[( Timestamp, String, Double, Double, Double, Double, Double, Double)]


    tableEnv.registerDataStream("RSIsignal_table_NaN", RSIsignal_table2, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'RS , 'RS1, 'RSI, 'UserActionTime.proctime )

    val rsi_table_3 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, " +
      "                                 RS1, " +
      "                                   SUM(RS1) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - RS1  AS RSlag, RSI" +

      "                                   FROM RSIsignal_table_NaN")

    val RSIsignal_table3 = rsi_table_3.toAppendStream[( Timestamp, String, Double, Double, Double, Double, Double, Double)]


    tableEnv.registerDataStream("RSIsignal_table2", RSIsignal_table3, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'RS ,'RSlag, 'RSI, 'UserActionTime.proctime )

    val rsi_lag_table = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, RSI," +
      "                                    SUM(RSI)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - RSI  as RSIlag " +
      "                                    FROM RSIsignal_table2" )

    val rsi_lag_stream = rsi_lag_table.toAppendStream[( Timestamp,String, Double,Double, Double)]

    tableEnv.registerDataStream("lagRSI", rsi_lag_stream, 'stockTime, 'stockName, 'lastPrice, 'RSI, 'RSIlag, 'UserActionTime.proctime )


    /*
    Buy if RSI (t) < 30
    Sell if RSI (t) > 70
    Hold otherwise
    1 = BUY, 2 = SELL, 0 = HOLD

    remark: a lot of ones and twos after each other! (and too much signals in general) -> direction and signal function?

     */

    // big table to check the outcome:
    val RSI_signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName , lastPrice, ROUND(RSI,2)," +
      "                                       CASE WHEN RSI < 20 AND RSI > 0 AND RSI < RSIlag THEN 1 " +
      "                                       WHEN RSI > 80 AND RSI < 100 AND RSI > RSIlag THEN 2 ELSE 0 END as RSI_signal, " +
      "" +
      "                                       CASE WHEN RSI <= 30 THEN 1" +
      "                                       WHEN RSI >= RSIlag AND RSI > 30 AND RSI < 70 THEN 1 " +
      "                                       WHEN RSI >= 70 THEN -1" +
      "                                       WHEN RSI <= RSIlag AND RSI > 30 AND RSI < 70 THEN -1 ELSE 0 END AS RSI_direction  " +

      "                                       FROM  lagRSI" +
     // "                                       WHERE stockName = 'ABT UN Equity'" +
      " ")



    RSI_signal_table.toAppendStream[(RSITypes)]

    /* MODIFICATIONS:

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

     */

  }



}