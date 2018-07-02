package thesis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import thesis.financialFeatures._

object FeatureCalculation {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val tableEnv = TableEnvironment.getTableEnvironment(env)
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


  def  calculation (stream: DataStream[StockQuotes]): DataStream[MFITypes] = {

    // hier alle streams joinen naar één stream = basetable

    // SMA signal is calculated
    val sma10 = SMA.calculateSMA(stream, tableEnv, env)
    // SMA.calculateSMA(stream, tableEnv, env)

    // bb signal is calculated
    val bb = BolBand.calculateBolBand(stream, tableEnv, env)

    //CCI signal is calculated
    val cci = CCI.calculateCCI(stream, tableEnv, env)

    // sotch is calculated
    val stoch = Stoch.calculateStoch(stream, tableEnv, env)

    // RSI calculated
    val rsi = RSI.calculateRSI(stream, tableEnv, env)

    // MFI calculated
    val mfi = MFI.calculateMFI(stream, tableEnv, env)

    val chaikin = Chaikin.calculateChaikin(stream, tableEnv, env)

    val williamR = WilliamR.calculateWilliamR(stream, tableEnv, env)


    val wma = WMA.calculateWMA(stream, tableEnv, env)

    // SMA.calculateSMA(stream, tableEnv, env)
    // CCI.calculateCCI(stream, tableEnv, env)
    // BolBand.calculateBolBand(stream, tableEnv, env)
    // Stoch.calculateStoch(stream, tableEnv, env)
    // RSI.calculateRSI(stream, tableEnv, env)
    MFI.calculateMFI(stream, tableEnv, env)



  }

}
