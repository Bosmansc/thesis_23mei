package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes


case class CCItypes(stockTime: Timestamp, stockName: String, CCI_signal: Int, CCI_direction: Int)

object CCI {

  def calculateCCI(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[CCItypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime, 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    /* buy if CCI (t - 1) >= 100 and CCI (t) < -100
       sell  if CCI (t - 1) <= 100 and CCI (t) > 100
       hold otherwise
        */

    val CCI_1 = tableEnv.sqlQuery("SELECT stockTime, stockName, (high + low + lastPrice)/3 as typicalPrice," +
      "                            (high + low + lastPrice)/3 -  AVG((high + low + lastPrice)/3) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as numerator_CCI," +
      "                            " +

      "                          ABS( (high + low + lastPrice)/3 - AVG((high + low + lastPrice)/3) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) ) as deviation" +
      "                           " +

      "                          FROM stockTable")

    val CCI_1_table = CCI_1.toAppendStream[(Timestamp, String, Double, Double, Double)]


    tableEnv.registerDataStream("CCI_1_table", CCI_1_table, 'stockTime, 'stockName, 'typicalPrice, 'numerator_CCI, 'deviation, 'UserActionTime.proctime)

    val CCI_2_table = tableEnv.sqlQuery("SELECT stockTime, stockName, typicalPrice,  numerator_CCI, deviation,   AVG(deviation)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as MAD, " +
      "                                 66.6666666667* ( numerator_CCI/(AVG(deviation)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) ) as CCI" +
      "                                 FROM CCI_1_table")

    val CCI_3 = CCI_2_table.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)]


    // calculate signal

    tableEnv.registerDataStream("CCI_signal", CCI_3, 'stockTime, 'stockName, 'typicalPrice, 'numerator_CCI, 'deviation, 'MAD, 'CCI, 'UserActionTime.proctime)

    val CCI_signal = tableEnv.sqlQuery("SELECT stockTime, stockName,  case when CCI > 0 OR CCI < 0 OR CCI =0 then CCI else 0 end" +
      "                                 FROM CCI_signal" +
      "                                 ")


    val CCI_signal_2 = CCI_signal.toAppendStream[(Timestamp, String, Double)]

    tableEnv.registerDataStream("CCI_signal_2", CCI_signal_2, 'stockTime, 'stockName, 'CCI, 'UserActionTime.proctime)

    val CCI_signal_t = tableEnv.sqlQuery("SELECT stockTime, stockName,  CCI, SUM(CCI)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - CCI as CCIlag " +
      "                                 FROM CCI_signal_2" +
      "                                 ")



    /* buy if CCI (t - 1) >= -100 and CCI (t) < -100
    sell  if CCI (t - 1) <= 100 and CCI (t) > 100
    hold otherwise
    1 = BUY, 2 = SELL, 0 = HOLD
     */

    val CCI_signal_3 = CCI_signal_t.toAppendStream[(Timestamp, String, Double, Double)]

    tableEnv.registerDataStream("CCI_signal_3", CCI_signal_3, 'stockTime, 'stockName, 'CCI, 'CCIlag, 'UserActionTime.proctime)

    // table to check if the signal is correct:
    val CCI_signal_3t = tableEnv.sqlQuery("SELECT stockTime, stockName,  CCI , CCIlag, " +
      "                                   CASE WHEN CCIlag >= -100 AND CCI < -100  THEN 1 " +
      "                                   WHEN CCIlag <= 100 AND CCI > 100 THEN 2 ELSE 0 END as SMA_signal" +
      "                                   FROM CCI_signal_3" +
      //   "                                    WHERE stockName = 'ABBV UN Equity' " +
      "                                 ")


    // CCI signal:

    val CCI_signal4 = tableEnv.sqlQuery("SELECT stockTime, stockName, " +
      "                                   CASE WHEN CCIlag >= -100 AND CCI < -100  THEN 1 " +
      "                                   WHEN CCIlag <= 100 AND CCI > 100 THEN 2 ELSE 0 END as SMA_signal, " +
      "" +
      "                                   CASE WHEN CCI <= -200 THEN 1" +
      "                                   WHEN CCI > CCIlag AND CCI < 200 AND CCI > -200 THEN 1" +
      "                                   WHEN CCI >= 200 THEN -1" +
      "                                   WHEN CCI <= CCIlag AND CCI < 200 AND CCI > -200 THEN -1 ELSE 0 END AS CCI_direction" +
      "                                    " +
      "                                   FROM CCI_signal_3")


    val CCIsignal = CCI_signal4.toAppendStream[(CCItypes)]

    CCI_signal4.toAppendStream[(CCItypes)]


  }


}
