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


  def  calculation (stream: DataStream[StockQuotes]): DataStream[(Timestamp, String, Double,Double, String, Int,Int, String, Int,Int, String, Int,Int, String, Int, Int,String, Int,Int, String, Int, Int,String, Int,Int, String,Int, Int)] = {
 // def  calculation (stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double,Double, Int, Int,  Int, Int,  Int,  Int,  Int,  Int, Int)] = {

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



    // **************************** merge all signals to BASETABLE and add responseVariable based on threshold for STREAMING model: ****************************
    // eigenlijk moet hier geen responsevariabele zijn!!

    val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
      "                                     ' SMA: ', SMA10.SMA_signal, SMA10.SMA_direction,  ' BB: ', bb.BB_signal, BB_direction, ' CCI: ', cci.CCI_signal, CCI_direction, " +
      "                                     ' stoch: ', stoch.stoch_signal, stoch_direction, ' RSI: ', rsi.RSI_signal, RSI_direction, ' MFI: ', mfi.MFI_signal, moneyFlowIndex_direction," +
      "                                     ' Chaikin: ', chaikin.chaikin_signal, chaikin_direction, ' williamR: ', williamR.willR_signal, williamsR_direction," +

      "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

      "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
      "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
      "                                     AND chaikin.stockTime = williamR.stockTime AND" +
      "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
      "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
      "                                     AND chaikin.stockName = williamR.stockName" +
      "                                     " +
       "                                      AND williamR.stockName ='AAPL UW Equity' " +
      "                                   " )




    baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int,Int, String, Int,Int, String, Int,Int, String, Int,Int, String, Int, Int,String, Int,Int, String, Int,Int, String, Int,Int)]
    val BaseTableStream =  baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int,Int, String, Int,Int, String, Int,Int, String, Int,Int, String,Int, Int, String, Int,Int, String, Int,Int, String,Int, Int)]

    tableEnv.registerDataStream("BaseTableBatch", BaseTableStream, 'stockTime, 'stockName, 'lastPrice,'lastPriceLag, 'SMAS, 'SMA_signal, 'BB, 'BB_signal, 'CCI, 'CCI_signal, 'stoch, 'stoch_signal, 'RSI, 'RSI_signal, 'MFI, 'MFI_signal,
      'Chaikin, 'chaikin_signal, 'williamR, 'willR_signal, 'UserActionTime.proctime )


    // stream output:
    BaseTableStream




  }

}
