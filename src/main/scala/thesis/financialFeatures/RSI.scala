package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class RSITypes(stockTime: Timestamp, stockName: String,  RSI_signal: Int )

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
      "                                 ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ) as RS," +

      "                                  100 - 100/( 1 + ( AVG(posDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/( AVG(negDifference) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )) as RSI"  +

      "                                   FROM rsi_table_1")

    val RSIsignal_table = rsi_table_1.toAppendStream[( Timestamp, String, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("RSIsignal_table", RSIsignal_table, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'RS , 'RSI, 'UserActionTime.proctime )

    val rsi_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, posDifference, negDifference, " +
      "                                 RS, SUM(RS) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - RS as RSlag" +

      "                                   FROM RSIsignal_table")

    val RSIsignal_table2 = rsi_table_2.toAppendStream[( Timestamp, String, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("RSIsignal_table2", RSIsignal_table2, 'stockTime, 'stockName, 'lastPrice, 'posDifference, 'negDifference, 'RS , 'RSI, 'UserActionTime.proctime )





    /*
    Buy if RSI (t) < 30
    Sell if RSI (t) > 70
    Hold otherwise
    1 = BUY, 2 = SELL, 0 = HOLD

    remark: a lot of ones and twos after each other! (and too much signals in general) -> direction and signal function?

     */

    // big table to check the outcome:
    val RSI_signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName , lastPrice, ROUND(RSI,2)," +
      "                                       CASE WHEN RSI < 30 THEN 1 " +
      "                                       WHEN RSI > 70  THEN 2 ELSE 0 END as RSI_signal  " +

      "                                       FROM  RSIsignal_table" +
      "                                       WHERE stockName = 'ABT UN Equity' ")

    // RSI_signal_table.toAppendStream[(RSITypes)]

    // signal:
    val RSI_signal = tableEnv.sqlQuery("SELECT stockTime, stockName ," +
      "                                       CASE WHEN RSI < 30 THEN 1 " +
      "                                       WHEN RSI > 70  THEN 2 ELSE 0 END as RSI_signal  " +

      "                                       FROM  RSIsignal_table")

    RSI_signal.toAppendStream[(RSITypes)]

  }



}