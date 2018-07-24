
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
  def  calculation (stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double,Double, Int, Int,  Int, Int,  Int,  Int,  Int,  Int, Int)] = {

    // hier alle streams joinen naar één stream = basetable

    // SMA signal is calculated
    val sma10 = SMA.calculateSMA(stream, tableEnv, env)
    tableEnv.registerDataStream("SMA10", sma10, 'stockTime, 'stockName,'lastPirce, 'SMA_signal,'SMA_direction, 'UserActionTime.proctime )

    // bb signal is calculated
    val bb = BolBand.calculateBolBand(stream, tableEnv, env)
    tableEnv.registerDataStream("bb", bb, 'stockTime, 'stockName,'lastPrice, 'lastPriceLag, 'BB_signal,'BB_direction, 'UserActionTime.proctime )

    //CCI signal is calculated
    val cci = CCI.calculateCCI(stream, tableEnv, env)
    tableEnv.registerDataStream("cci", cci, 'stockTime, 'stockName, 'CCI_signal,'CCI_direction, 'UserActionTime.proctime )

    // stoch is calculated
    val stoch = Stoch.calculateStoch(stream, tableEnv, env)
    tableEnv.registerDataStream("stoch", stoch, 'stockTime, 'stockName, 'stoch_signal, 'stoch_direction, 'UserActionTime.proctime )

    // RSI calculated
    val rsi = RSI.calculateRSI(stream, tableEnv, env)
    tableEnv.registerDataStream("rsi", rsi, 'stockTime, 'stockName, 'lastPrice, 'RSI, 'RSI_signal,'RSI_direction, 'UserActionTime.proctime )

    // MFI calculated (here lastPrice is added)
    val mfi = MFI.calculateMFI(stream, tableEnv, env)
    tableEnv.registerDataStream("mfi", mfi, 'stockTime, 'stockName, 'lastPrice,'moneyFlowIndex, 'MFI_signal, 'moneyFlowIndex_direction, 'UserActionTime.proctime )

    // chaiking calculated
    val chaikin = Chaikin.calculateChaikin(stream, tableEnv, env)
    tableEnv.registerDataStream("chaikin", chaikin, 'stockTime, 'stockName, 'lastPrice, 'chaikin, 'chaikin_signal, 'chaikin_direction, 'UserActionTime.proctime )

    // williamR calculated
    val williamR = WilliamR.calculateWilliamR(stream, tableEnv, env)
    tableEnv.registerDataStream("williamR", williamR, 'stockTime, 'stockName, 'lastPrice, 'williamsR, 'willR_signal, 'williamsR_direction,'UserActionTime.proctime )

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

    val batchStream = BaseTableBatch2.toAppendStream[(Timestamp, String, Double,Double, Int, Int,  Int, Int,  Int,  Int,  Int,  Int, Int)]
    batchStream





  }

}
