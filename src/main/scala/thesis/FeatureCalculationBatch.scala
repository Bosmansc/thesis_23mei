
package thesis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.financialFeatures._

object FeatureCalculationBatch {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val tableEnv = TableEnvironment.getTableEnvironment(env)
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


  // def  calculation (stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)] = {
  def calculation(stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double, Double, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)] = {

    // hier alle streams joinen naar één stream = basetable

    // SMA signal is calculated
    val sma10 = SMA.calculateSMA(stream, tableEnv, env)
    tableEnv.registerDataStream("SMA10", sma10, 'stockTime, 'stockName, 'lastPrice, 'SMA_signal, 'SMA_direction, 'UserActionTime.proctime)

    // bb signal is calculated
    val bb = BolBand.calculateBolBand(stream, tableEnv, env)
    tableEnv.registerDataStream("bb", bb, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag, 'BB_signal, 'BB_direction, 'UserActionTime.proctime)

    //CCI signal is calculated
    val cci = CCI.calculateCCI(stream, tableEnv, env)
    tableEnv.registerDataStream("cci", cci, 'stockTime, 'stockName, 'CCI_signal, 'CCI_direction, 'UserActionTime.proctime)

    // stoch is calculated
    val stoch = Stoch.calculateStoch(stream, tableEnv, env)
    tableEnv.registerDataStream("stoch", stoch, 'stockTime, 'stockName, 'stoch_signal, 'stoch_direction, 'UserActionTime.proctime)

    // RSI calculated
    val rsi = RSI.calculateRSI(stream, tableEnv, env)
    tableEnv.registerDataStream("rsi", rsi, 'stockTime, 'stockName, 'lastPrice, 'RSI, 'RSI_signal, 'RSI_direction, 'UserActionTime.proctime)

    // MFI calculated (here lastPrice is added)
    val mfi = MFI.calculateMFI(stream, tableEnv, env)
    tableEnv.registerDataStream("mfi", mfi, 'stockTime, 'stockName, 'lastPrice, 'moneyFlowIndex, 'MFI_signal, 'moneyFlowIndex_direction, 'UserActionTime.proctime)

    // chaiking calculated
    val chaikin = Chaikin.calculateChaikin(stream, tableEnv, env)
    tableEnv.registerDataStream("chaikin", chaikin, 'stockTime, 'stockName, 'lastPrice, 'chaikin, 'chaikin_signal, 'chaikin_direction, 'UserActionTime.proctime)

    // williamR calculated
    val williamR = WilliamR.calculateWilliamR(stream, tableEnv, env)
    tableEnv.registerDataStream("williamR", williamR, 'stockTime, 'stockName, 'lastPrice, 'williamsR, 'willR_signal, 'williamsR_direction, 'UserActionTime.proctime)

    // WMA not calculated!
    //  val wma = WMA.calculateWMA(stream, tableEnv, env)

    // SMA.calculateSMA(stream, tableEnv, env)
    // CCI.calculateCCI(stream, tableEnv, env)
    // BolBand.calculateBolBand(stream, tableEnv, env)
    // Stoch.calculateStoch(stream, tableEnv, env)
    // RSI.calculateRSI(stream, tableEnv, env)
    // MFI.calculateMFI(stream, tableEnv, env)
    // Chaikin.calculateChaikin(stream, tableEnv, env)
    // WilliamR.calculateWilliamR(stream, tableEnv, env)
    // WMA.calculateWMA(stream, tableEnv, env)


    // **************************** stream for batch processing to generate the ML model ****************************

    val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, bb.lastPriceLag," +
      "                                      SMA10.SMA_signal, SMA10.SMA_direction,  bb.BB_signal, BB_direction,  cci.CCI_signal, CCI_direction, " +
      "                                      stoch.stoch_signal, stoch_direction,  rsi.RSI_signal, RSI_direction,  mfi.MFI_signal, moneyFlowIndex_direction," +
      "                                      chaikin.chaikin_signal, chaikin_direction,  williamR.willR_signal, williamsR_direction" +

      "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

      "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
      "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
      "                                     AND chaikin.stockTime = williamR.stockTime AND" +
      "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
      "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
      "                                     AND chaikin.stockName = williamR.stockName" +
      "                                     " +
      //     "                                      AND williamR.stockName ='AAPL UW Equity' " +
      "                                   ")

    val BaseTableStream = baseTable.toAppendStream[(Timestamp, String, Double, Double, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]

    tableEnv.registerDataStream("BaseTableBatch", BaseTableStream, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag, 'SMA_signal, 'SMA_direction, 'BB_signal, 'BB_direction, 'CCI_signal, 'CCI_direction,
      'stoch_signal, 'stoch_direction, 'RSI_signal, 'RSI_direction, 'MFI_signal, 'moneyFlowIndex_direction,
      'chaikin_signal, 'chaikin_direction, 'willR_signal, 'williamsR_direction, 'UserActionTime.proctime)


    // ONLY USE THIS FOR BATCH TRAINING, NOT FOR SCORING, ALL LABELS ARE ONE ROW LAGGED!
    val BaseTableBatch2 = tableEnv.sqlQuery(
      s"""
         |SELECT stockTime, stockName, lastPrice, lastPriceLag,
         | SUM(SMA_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - SMA_signal,
         | SUM(SMA_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - SMA_direction,
         | SUM(BB_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - BB_signal,
         | SUM(BB_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - BB_direction,
         | SUM(CCI_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - CCI_signal,
         | SUM(CCI_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - CCI_direction,
         | SUM(stoch_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - stoch_signal,
         | SUM(stoch_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - stoch_direction,
         | SUM(chaikin_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - chaikin_signal,
         | SUM(chaikin_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - chaikin_direction,
         | SUM(willR_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - willR_signal,
         | SUM(williamsR_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - williamsR_direction,
         | SUM(RSI_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - RSI_signal,
         | SUM(RSI_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - RSI_direction,
         | SUM(MFI_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - MFI_signal,
         | SUM(moneyFlowIndex_direction) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - moneyFlowIndex_direction,
         |
                                      | CASE WHEN lastPrice - lastPriceLag >= $threshold THEN 1
         |  WHEN lastPrice - lastPriceLag <=  -$threshold THEN 2 ELSE 0 END AS responseVariable

         |
                                      |FROM BaseTableBatch
         |

                                    """.stripMargin)

    val batchStream = BaseTableBatch2.toAppendStream[(Timestamp, String, Double, Double, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]
    batchStream


  }

}
