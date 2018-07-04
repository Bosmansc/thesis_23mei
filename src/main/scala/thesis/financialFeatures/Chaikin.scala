package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class ChaikinTypes(stockTime: Timestamp, stockName: String,  chaikin_signal:Int )

object Chaikin {

  def calculateChaikin(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[ChaikinTypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    // Chaikin Accumulation/Distribution

    //   [(Close  -  Low) - (High - Close)] /(High - Low) = Money Flow Multiplier


    val chai = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, volume, ( (lastPrice - low) - (high - lastPrice) )/( high - low) as moneyFlowMultiplier," +
      "                           CASE WHEN (high-low) > 0 THEN ( ( (lastPrice - low) - (high - lastPrice) )/( high - low) ) * volume ELSE 0 END as moneyFlowVolume" +
      "                            FROM stockTable ")

    val chai_tbl = chai.toAppendStream[(Timestamp, String, Double,Double, Double, Double)]



    tableEnv.registerDataStream("chai_tbl_1", chai_tbl, 'stockTime, 'stockName, 'lastPrice, 'volume, 'moneyFlowMultiplier, 'moneyFlowVolume, 'UserActionTime.proctime )

    // Round hier nog fixen -> Nan wegwerken
    val chai_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, moneyFlowMultiplier, moneyFlowVolume, " +

      "                                  ( SUM(moneyFlowVolume) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) )/" +
      "                                  ( SUM(volume) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) ) as chaikinMoneyFlow" +
      "                                   FROM chai_tbl_1 " )

    val chai_stream = chai_table_2.toAppendStream[(Timestamp, String, Double, Double, Double, Double)]

    // lag table:
    tableEnv.registerDataStream("chai_lag", chai_stream, 'stockTime, 'stockName, 'lastPrice,  'moneyFlowMultiplier, 'moneyFlowVolume, 'chaikinMoneyFlow, 'UserActionTime.proctime )

    val chaikin_lag = tableEnv.sqlQuery("SELECT stockTime, stockName,  lastPrice, chaikinMoneyFlow, SUM(chaikinMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - chaikinMoneyFlow as chaikinMoneyFlowLag" +
      "                             " +
      "                             FROM chai_lag ")


    val chaikin_lag_table = chaikin_lag.toAppendStream[(Timestamp, String, Double, Double, Double)]

    /*
    Buy if Chaikin (t - 1) <= 0 and Chaikin (t) > 0
    Sell if Chaikin (t - 1) >= 0 and Chaikin (t) < 0
    Hold otherwise
    1 = BUY, 2 = SELL, 0 = HOLD
     */

    tableEnv.registerDataStream("chaikin_big_table", chaikin_lag_table, 'stockTime, 'stockName, 'lastPrice,  'chaikinMoneyFlow, 'chaikinMoneyFlowLag, 'UserActionTime.proctime )

    //table to check outcome:
    val chaikin_signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, ROUND(chaikinMoneyFlow,2), ROUND(chaikinMoneyFlowLag,2)," +
      "                                       CASE WHEN chaikinMoneyFlowLag <= 0 AND chaikinMoneyFlow > 0 THEN 1 " +
      "                                       WHEN chaikinMoneyFlowLag >= 0 AND chaikinMoneyFlow < 0 THEN 2 ELSE 0 END as chaikin_signal" +
      "                                       FROM chaikin_big_table" +
      "                                       WHERE stockName = 'AAPL UW Equity'" +
      "                                        ")


    // signal: (21 iterations needed for useful results)
    val chaikin_signal = tableEnv.sqlQuery("SELECT stockTime, stockName," +
      "                                       CASE WHEN chaikinMoneyFlowLag <= 0 AND chaikinMoneyFlow > 0 THEN 1 " +
      "                                       WHEN chaikinMoneyFlowLag >= 0 AND chaikinMoneyFlow < 0 THEN 2 ELSE 0 END as chaikin_signal" +
      "                                       FROM chaikin_big_table" +
      "                                        ")


    chaikin_signal.toAppendStream[(ChaikinTypes)]


  }



}
