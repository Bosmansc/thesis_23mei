package thesis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.financialFeatures._

object FeatureCalculation {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val tableEnv = TableEnvironment.getTableEnvironment(env)
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


 // def  calculation (stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)] = {
  def  calculation (stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double,Double, Int, Int,  Int, Int,  Int,  Int,  Int,  Int, Int)] = {

    // hier alle streams joinen naar één stream = basetable

    // SMA signal is calculated
    val sma10 = SMA.calculateSMA(stream, tableEnv, env)
    tableEnv.registerDataStream("SMA10", sma10, 'stockTime, 'stockName, 'SMA_signal, 'UserActionTime.proctime )

    // bb signal is calculated
    val bb = BolBand.calculateBolBand(stream, tableEnv, env)
    tableEnv.registerDataStream("bb", bb, 'stockTime, 'stockName, 'lastPriceLag, 'BB_signal, 'UserActionTime.proctime )

    //CCI signal is calculated
    val cci = CCI.calculateCCI(stream, tableEnv, env)
    tableEnv.registerDataStream("cci", cci, 'stockTime, 'stockName, 'CCI_signal, 'UserActionTime.proctime )

    // stoch is calculated
    val stoch = Stoch.calculateStoch(stream, tableEnv, env)
    tableEnv.registerDataStream("stoch", stoch, 'stockTime, 'stockName, 'stoch_signal, 'UserActionTime.proctime )

    // RSI calculated
    val rsi = RSI.calculateRSI(stream, tableEnv, env)
    tableEnv.registerDataStream("rsi", rsi, 'stockTime, 'stockName, 'RSI_signal, 'UserActionTime.proctime )

    // MFI calculated (here lastPrice is added)
    val mfi = MFI.calculateMFI(stream, tableEnv, env)
    tableEnv.registerDataStream("mfi", mfi, 'stockTime, 'stockName, 'lastPrice, 'MFI_signal, 'UserActionTime.proctime )

    // chaiking calculated
    val chaikin = Chaikin.calculateChaikin(stream, tableEnv, env)
    tableEnv.registerDataStream("chaikin", chaikin, 'stockTime, 'stockName, 'chaikin_signal, 'UserActionTime.proctime )

    // williamR calculated
    val williamR = WilliamR.calculateWilliamR(stream, tableEnv, env)
    tableEnv.registerDataStream("williamR", williamR, 'stockTime, 'stockName, 'willR_signal, 'UserActionTime.proctime )

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



    // ************** merge all signals to BASETABLE and add responseVariable based on threshold: **************

    val threshold = threshold

    //voorlopig zonder threshold: THRESHOLD VIA DOLLAR TEKEN DOEN!
    val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
      "                                     ' SMA: ', SMA10.SMA_signal, ' BB: ', bb.BB_signal, ' CCI: ', cci.CCI_signal, ' stoch: ', stoch.stoch_signal, ' RSI: ', rsi.RSI_signal, ' MFI: ', mfi.MFI_signal," +
      "                                     ' Chaikin: ', chaikin.chaikin_signal, ' williamR: ', williamR.willR_signal," +

      s"                                     CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  $threshold THEN 1 " +
      s"                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -$threshold THEN 2 ELSE 0 END as responseVariable" +

      "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

      "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
      "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
      "                                     AND chaikin.stockTime = williamR.stockTime AND" +
      "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
      "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
      "                                     AND chaikin.stockName = williamR.stockName" +
      "                                     " +
      // "                                      AND williamR.stockName ='AAPL UW Equity' " +
      "                                   " )
    /*  "" +
      "                                     AND CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  0.1 THEN 1                         " +
      "                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -0.1 THEN 2 ELSE 0 END > 0 ")*/




    baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]
    val BaseTableBatch =  baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]

    tableEnv.registerDataStream("BaseTableBatch", BaseTableBatch, 'stockTime, 'stockName, 'lastPrice,'lastPriceLag, 'SMAS, 'SMA_signal, 'BB, 'BB_signal, 'CCI, 'CCI_signal, 'stoch, 'stoch_signal, 'RSI, 'RSI_signal, 'MFI, 'MFI_signal,
      'Chaikin, 'chaikin_signal, 'williamR, 'willR_signal, 'responseVariable, 'UserActionTime.proctime )

    // ONLY USE THIS FOR BATCH TRAINING, NOT FOR SCORING, ALL LABELS ARE ONE ROW LAGGED!
    val BaseTableBatch2 = tableEnv.sqlQuery(
      """
                                      |SELECT stockTime, stockName, lastPrice, lastPriceLag,
                                      | SUM(SMA_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - SMA_signal,
                                      | SUM(BB_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - BB_signal,
                                      | SUM(CCI_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - CCI_signal,
                                      | SUM(stoch_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - stoch_signal,
                                      | SUM(chaikin_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - chaikin_signal,
                                      | SUM(willR_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - willR_signal,
                                      | SUM(RSI_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - RSI_signal,
                                      | SUM(MFI_signal) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - MFI_signal,
                                      | responseVariable

                                      |
                                      |FROM BaseTableBatch
                                      |

                                    """.stripMargin)

    BaseTableBatch2.toAppendStream[(Timestamp, String, Double,Double, Int, Int,  Int, Int,  Int,  Int,  Int,  Int, Int)]




  }

}
