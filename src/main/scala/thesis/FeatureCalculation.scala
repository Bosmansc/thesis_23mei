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


  def  calculation (stream: DataStream[StockQuotes]): DataStream[BolBandtypes] = {

    // hier alle streams joinen naar één stream = basetable (nog oplossing zoeken voor mergen)

    val SMA10 = SMA.calculateSMA(stream, tableEnv, env)

    val BB = BolBand.calculateBolBand(stream, tableEnv, env)
    BolBand.calculateBolBand(stream, tableEnv, env)


    /* merge all the streams this way:
    val merg = tableEnv.sqlQuery("SELECT *" +
      "                           FROM mfi_table_2 INNER JOIN mfi_table_1 ON mfi_table_2.stockTime = mfi_table_1.stockTime")

    val merg_str = merg.toAppendStream[(Timestamp, String, Double, Double, Double, Timestamp, Timestamp, String, Double, Double, Double, Timestamp)]

    */




  }

}
