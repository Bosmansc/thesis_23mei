package thesis


import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.financialFeatures._

case class SignalAndDirectionTypes(SMA10:Double, SMA100:Double, SMA_signal: Int, SMA_direction: Int, BB_lowerBound:Double, BB_upperBound:Double, BB_middleBand:Double, BB_signal: Int, BB_direction: Int, CCI:Double, CCI_signal: Int, CCI_direction: Int,
                                   D:Double, stoch_signal: Int, stoch_direction: Int, RSI:Double, RSI_signal: Int, RSI_direction: Int, moneyFlowIndex:Double, MFI_signal: Int, moneyFlowIndex_direction: Int,
                                   chaikin:Double, chaikin_signal: Int, chaikin_direction: Int, williamsR:Double, willR_signal: Int, williamsR_direction: Int) {

  def toVector = DenseVector(SMA_signal, SMA_direction, BB_signal, BB_direction, CCI_signal, CCI_direction,
    stoch_signal, stoch_direction, RSI_signal, RSI_direction, MFI_signal, moneyFlowIndex_direction,
    chaikin_signal, chaikin_direction, willR_signal, williamsR_direction)
}

object FeatureCalculation {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val tableEnv = TableEnvironment.getTableEnvironment(env)
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


  def calculation(stream: DataStream[StockQuotes]): DataStream[SignalAndDirectionTypes] = {
    // def  calculation (stream: DataStream[StockQuotes]): DataStream[(Timestamp,String,  Double,Double,  Int,Int,  Int,Int,  Int,  Int, Int, Int,Int, Int, Int, Int,Int,Int, Int, Int)] = {
    // def  calculation (stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double,Double, Int, Int,  Int, Int,  Int,  Int,  Int,  Int, Int)] = {

    // hier alle streams joinen naar één stream = basetable

    // SMA signal is calculated
    val sma10 = SMA.calculateSMA(stream, tableEnv, env)
    tableEnv.registerDataStream("SMA10", sma10, 'stockTime, 'stockName, 'lastPrice, 'SMA10, 'SMA100, 'SMA_signal, 'SMA_direction, 'UserActionTime.proctime)

    // bb signal is calculated
    val bb = BolBand.calculateBolBand(stream, tableEnv, env)
    tableEnv.registerDataStream("bb", bb, 'stockTime, 'stockName, 'lastPrice, 'lastPriceLag,'BB_lowerBound, 'BB_upperBound, 'BB_middleBand, 'BB_signal, 'BB_direction, 'UserActionTime.proctime)

    //CCI signal is calculated
    val cci = CCI.calculateCCI(stream, tableEnv, env)
    tableEnv.registerDataStream("cci", cci, 'stockTime, 'stockName, 'CCI, 'CCI_signal, 'CCI_direction, 'UserActionTime.proctime)

    // stoch is calculated
    val stoch = Stoch.calculateStoch(stream, tableEnv, env)
    tableEnv.registerDataStream("stoch", stoch, 'stockTime, 'stockName, 'D, 'stoch_signal, 'stoch_direction, 'UserActionTime.proctime)

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


    // **************************** merge all signals to BASETABLE and add responseVariable based on threshold for STREAMING model: ****************************


    val baseTable = tableEnv.sqlQuery("SELECT " +
      "                                      SMA10.SMA10, SMA10.SMA100, SMA10.SMA_signal, SMA10.SMA_direction, BB_lowerBound, BB_upperBound, BB_middleBand, bb.BB_signal, BB_direction, CCI,  cci.CCI_signal, CCI_direction, " +
      "                                     D, stoch.stoch_signal, stoch_direction, RSI, rsi.RSI_signal, RSI_direction, moneyFlowIndex, mfi.MFI_signal, moneyFlowIndex_direction," +
      "                                     chaikin, chaikin.chaikin_signal, chaikin_direction, williamsR, williamR.willR_signal, williamsR_direction" +

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


    val BaseTableStream = baseTable.toAppendStream[(SignalAndDirectionTypes)]



    // stream output:
    // baseTable.toAppendStream[(  Int,Int,  Int,Int,  Int,Int,  Int, Int, Int,Int, Int, Int, Int,Int,Int, Int)]
    baseTable.toAppendStream[(SignalAndDirectionTypes)]


  }

}
