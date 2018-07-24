package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class ChaikinTypes(stockTime: Timestamp, stockName: String, lastPrice:Double, chaikin: Double,  chaikin_signal:Int, chaikin_direction:Int )

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

    //10 ADL:
    tableEnv.registerDataStream("chai_ad3", chaikin_lag_table, 'stockTime, 'stockName, 'lastPrice, 'chaikinMoneyFlow, 'chaikinMoneyFlowLag, 'UserActionTime.proctime )

    val chaikin_ADL10 = tableEnv.sqlQuery("SELECT stockTime, stockName,  lastPrice, chaikinMoneyFlow, chaikinMoneyFlowLag, " +
      "                                 AVG(chaikinMoneyFlow) OVER ( PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) as ADL10" +
      "                             " +
      "                             FROM chai_ad3 ")
    val chaikin_ADL10_table = chaikin_ADL10.toAppendStream[(Timestamp, String, Double, Double, Double, Double)]



    //3 ADL:
    tableEnv.registerDataStream("chai_ad10", chaikin_ADL10_table, 'stockTime, 'stockName, 'lastPrice, 'chaikinMoneyFlow,'chaikinMoneyFlowLag,'ADL10, 'UserActionTime.proctime )

    val chaikin_ADL3 = tableEnv.sqlQuery("SELECT stockTime, stockName,  lastPrice, chaikinMoneyFlow, chaikinMoneyFlowLag, ADL10," +
      "                                 AVG(chaikinMoneyFlow) OVER ( PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as ADL3" +
      "                             " +
      "                             FROM chai_ad10 ")

    val chaikin_ADL3_table = chaikin_ADL3.toAppendStream[(Timestamp, String, Double, Double, Double, Double, Double)]


    /*
    Buy if Chaikin (t - 1) <= 0 and Chaikin (t) > 0
    Sell if Chaikin (t - 1) >= 0 and Chaikin (t) < 0
    Hold otherwise
    1 = BUY, 2 = SELL, 0 = HOLD
     */

    tableEnv.registerDataStream("chaikin_ADL3_table", chaikin_ADL3_table, 'stockTime, 'stockName, 'lastPrice,  'chaikinMoneyFlow, 'chaikinMoneyFlowLag, 'ADL10, 'ADL3, 'UserActionTime.proctime )

    val chaikin = tableEnv.sqlQuery("SELECT stockTime, stockName,  lastPrice, chaikinMoneyFlow, " +
      "                                   ADL3 - ADL10 as chaikin" +
      "                             " +
      "                             FROM chaikin_ADL3_table ")

    val chaikin_stream = chaikin.toAppendStream[(Timestamp, String, Double, Double, Double)]

    // lag table:
    tableEnv.registerDataStream("chaikin_lag", chaikin_stream, 'stockTime, 'stockName, 'lastPrice,  'chaikinMoneyFlow, 'chaikin, 'UserActionTime.proctime )

    val chaiking_lag = tableEnv.sqlQuery("SELECT stockTime, stockName,  lastPrice, chaikin, SUM(chaikin) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - chaikin as chaikinLag" +
      "                             " +
      "                             FROM chaikin_lag ")


    val chaikin_lag_stream = chaiking_lag.toAppendStream[(Timestamp, String, Double, Double, Double)]

    tableEnv.registerDataStream("chaiking", chaikin_lag_stream, 'stockTime, 'stockName, 'lastPrice,  'chaikin, 'chaikinLag, 'UserActionTime.proctime )


    //table to check outcome:
    val chaikin_signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, chaikin ," +
      "                                       CASE WHEN chaikinLag <= 0 AND chaikin > 0 THEN 1 " +
      "                                       WHEN chaikinLag >= 0 AND chaikin < 0 THEN 2 ELSE 0 END as chaikin_signal," +
      "" +
      "                                       CASE WHEN chaikin > 0 then 1 WHEN chaikin <= 0 then -1 ELSE 0 END as chaikin_direction" +

      "                                       FROM chaiking" +
   //   "                                       WHERE stockName = 'AAPL UW Equity'" +
      "                                        ")

    val chaikin_signalAndDirection = chaikin_signal_table.toAppendStream[(ChaikinTypes)]

    chaikin_signal_table.toAppendStream[(ChaikinTypes)]


  }



}
