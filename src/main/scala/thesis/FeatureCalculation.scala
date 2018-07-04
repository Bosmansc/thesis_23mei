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


  def  calculation (stream: DataStream[StockQuotes], threshold: Double): DataStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)] = {

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

    if (threshold <= 0.01) {

      val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
        "                                     SMA10.SMA_signal, bb.BB_signal, cci.CCI_signal, stoch.stoch_signal, rsi.RSI_signal, mfi.MFI_signal," +
        "                                     chaikin.chaikin_signal, williamR.willR_signal," +

        "                                     CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  0.01 THEN 1 " +
        "                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -0.01 THEN 2 ELSE 0 END as responseVariable" +

        "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

        "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
        "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
        "                                     AND chaikin.stockTime = williamR.stockTime AND" +
        "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
        "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
        "                                     AND chaikin.stockName = williamR.stockName")

      baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]



    } else if (0.01 < threshold && threshold <= 0.1) {

      val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
        "                                     ' SMA: ', SMA10.SMA_signal, ' BB: ', bb.BB_signal, ' CCI: ', cci.CCI_signal, ' stoch: ', stoch.stoch_signal, ' RSI: ', rsi.RSI_signal, ' MFI: ', mfi.MFI_signal," +
        "                                     ' Chaikin: ', chaikin.chaikin_signal, ' williamR: ', williamR.willR_signal," +

        "                                     CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  0.1 THEN 1 " +
        "                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -0.1 THEN 2 ELSE 0 END as responseVariable" +

        "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

        "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
        "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
        "                                     AND chaikin.stockTime = williamR.stockTime AND" +
        "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
        "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
        "                                     AND chaikin.stockName = williamR.stockName" +
        "                                     AND SMA10.stockName ='ABBV UN Equity'")

      baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]

    } else if (threshold <= 0.2 && threshold > 0.1) {

      val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
        "                                     'SMA: ', SMA10.SMA_signal, 'BB: ', bb.BB_signal, 'CCI: ', cci.CCI_signal, 'stoch: ', stoch.stoch_signal, 'RSI: ', rsi.RSI_signal, 'MFI: ', mfi.MFI_signal," +
        "                                     'Chaikin: ', chaikin.chaikin_signal, 'williamR: ', williamR.willR_signal," +

        "                                     CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  0.2 THEN 1 " +
        "                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -0.2 THEN 2 ELSE 0 END as responseVariable" +

        "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

        "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
        "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
        "                                     AND chaikin.stockTime = williamR.stockTime AND" +
        "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
        "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
        "                                     AND chaikin.stockName = williamR.stockName" +
        "                                     AND SMA10.stockName ='ABBV UN Equity'")

      baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]


    } else if (threshold <= 0.5 && threshold > 0.2) {

      val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
        "                                     'SMA: ', SMA10.SMA_signal, 'BB: ', bb.BB_signal, 'CCI: ', cci.CCI_signal, 'stoch: ', stoch.stoch_signal, 'RSI: ', rsi.RSI_signal, 'MFI: ', mfi.MFI_signal," +
        "                                     'Chaikin: ', chaikin.chaikin_signal, 'williamR: ', williamR.willR_signal," +

        "                                     CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  0.5 THEN 1 " +
        "                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -0.5 THEN 2 ELSE 0 END as responseVariable" +

        "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

        "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
        "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
        "                                     AND chaikin.stockTime = williamR.stockTime AND" +
        "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
        "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
        "                                     AND chaikin.stockName = williamR.stockName")

      baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]

    }

    else if (threshold <= 0.75 && threshold > 0.5) {
      val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
        "                                     'SMA: ', SMA10.SMA_signal, 'BB: ', bb.BB_signal, 'CCI: ', cci.CCI_signal, 'stoch: ', stoch.stoch_signal, 'RSI: ', rsi.RSI_signal, 'MFI: ', mfi.MFI_signal," +
        "                                     'Chaikin: ', chaikin.chaikin_signal, 'williamR: ', williamR.willR_signal," +

        "                                     CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  0.5 THEN 1 " +
        "                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -0.5 THEN 2 ELSE 0 END as responseVariable" +

        "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

        "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
        "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
        "                                     AND chaikin.stockTime = williamR.stockTime AND" +
        "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
        "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
        "                                     AND chaikin.stockName = williamR.stockName")

      baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]

    }


    else {

      val baseTable = tableEnv.sqlQuery("SELECT SMA10.stockTime, SMA10.stockName, mfi.lastPrice, ROUND(bb.lastPriceLag,2)," +
        "                                     ' SMA: ', SMA10.SMA_signal, 'BB: ', bb.BB_signal, 'CCI: ', cci.CCI_signal, 'stoch: ', stoch.stoch_signal, 'RSI: ', rsi.RSI_signal, 'MFI: ', mfi.MFI_signal," +
        "                                     'Chaikin: ', chaikin.chaikin_signal, 'williamR: ', williamR.willR_signal," +

        "                                     CASE WHEN mfi.lastPrice - bb.lastPriceLag >=  1 THEN 1 " +
        "                                     WHEN mfi.lastPrice - bb.lastPriceLag <=  -1 THEN 2 ELSE 0 END as responseVariable" +

        "                                     FROM SMA10, bb, cci, stoch, rsi, mfi, chaikin, williamR" +

        "                                     WHERE SMA10.stockTime = bb.stockTime AND bb.stockTime = cci.stockTime AND cci.stockTime = stoch.stockTime" +
        "                                     AND stoch.stockTime = rsi.stockTime AND rsi.stockTime = mfi.stockTime AND mfi.stockTime = chaikin.stockTime" +
        "                                     AND chaikin.stockTime = williamR.stockTime AND" +
        "                                     SMA10.stockName = bb.stockName AND bb.stockName = cci.stockName AND cci.stockName = stoch.stockName " +
        "                                     AND stoch.stockName = rsi.stockName AND rsi.stockName = mfi.stockName AND mfi.stockName = chaikin.stockName " +
        "                                     AND chaikin.stockName = williamR.stockName")

      baseTable.toAppendStream[(Timestamp, String, Double,Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]


    }






  }

}
