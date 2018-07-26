package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes


case class StochTypes(stockTime: Timestamp, stockName: String, stoch_signal: Int, stoch_direction: Int)

object Stoch {

  def calculateStoch(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[StochTypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime, 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)


    // Stochastic Oscillator

    val stoch = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, 100*((( lastPrice - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) )/" +
      "                           ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) ))) as K " +
      "                           FROM stockTable ")

    val stoch_table = stoch.toAppendStream[(Timestamp, String, Double, Double)]

    tableEnv.registerDataStream("stoch_table_2", stoch_table, 'stockTime, 'stockName, 'lastPrice, 'K, 'UserActionTime.proctime)

    val stoch_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, K, ( AVG(K)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) ) as D " +
      "                                    FROM stoch_table_2")

    val lagStoch = stoch_table_2.toAppendStream[(Timestamp, String, Double, Double, Double)]

    tableEnv.registerDataStream("lagStoch", lagStoch, 'stockTime, 'stockName, 'lastPrice, 'K, 'D, 'UserActionTime.proctime)

    val lag_stoch = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, D," +
      "                                    SUM(D)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - D  as DLag " +
      "                                    FROM lagStoch")

    val table_signal = lag_stoch.toAppendStream[(Timestamp, String, Double, Double, Double)]

    tableEnv.registerDataStream("table_signal", table_signal, 'stockTime, 'stockName, 'lastPrice, 'D, 'DLag, 'UserActionTime.proctime)

    /*
    Buy if %D(t - 1) >= 20 and %D(t) < 20
    Sell if %D(t - 1) <= 80 and %D(t) > 80
    Hold otherwise
     */

    // table to check the outcome:
    val signal_table_big = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, ROUND(D,2), ROUND(DLag,2)," +
      "                                     CASE WHEN DLag >= 20  AND D < 20  THEN 1 " +
      "                                     WHEN DLag <= 80 AND D > 80 THEN 2 ELSE 0 END as Stoch_signal" +
      "                                     FROM table_signal")
    //    "                                     WHERE stockName = 'ABT UN Equity'" )


    // signal:
    val signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName," +
      "                                     CASE WHEN DLag >= 20  AND D < 20  THEN 1 " +
      "                                     WHEN DLag <= 80 AND D > 80 THEN 2 ELSE 0 END as Stoch_signal," +
      "" +
      "                                     CASE WHEN DLag < D THEN 1" +
      "                                     WHEN DLag >= D THEN -1 ELSE 0 END as stoch_direction " +

      "                                     FROM table_signal")


    signal_table.toAppendStream[(StochTypes)]

  }


}
