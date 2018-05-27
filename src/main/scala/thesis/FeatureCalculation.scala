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


  def  calculation (stream: DataStream[StockQuotes]): DataStream[WMATypes] = {

    // hier alle streams joinen naar één stream = basetable (nog oplossing zoeken voor mergen)

    val sma10 = SMA.calculateSMA(stream, tableEnv, env)

    val bb = BolBand.calculateBolBand(stream, tableEnv, env)

    val cci = CCI.calculateCCI(stream, tableEnv, env)

    val stoch = Stoch.calculateStoch(stream, tableEnv, env)

    val rsi = RSI.calculateRSI(stream, tableEnv, env)

    val mfi = MFI.calculateMFI(stream, tableEnv, env)

    val chaikin = Chaikin.calculateChaikin(stream, tableEnv, env)

    val williamR = WilliamR.calculateWilliamR(stream, tableEnv, env)


    val wma = WMA.calculateWMA(stream, tableEnv, env)
    WMA.calculateWMA(stream, tableEnv, env)




    /* merge all the streams this way:
    val merg = tableEnv.sqlQuery("SELECT *" +
      "                           FROM mfi_table_2 INNER JOIN mfi_table_1 ON mfi_table_2.stockTime = mfi_table_1.stockTime")

    val merg_str = merg.toAppendStream[(Timestamp, String, Double, Double, Double, Timestamp, Timestamp, String, Double, Double, Double, Timestamp)]

    */




  }

}
