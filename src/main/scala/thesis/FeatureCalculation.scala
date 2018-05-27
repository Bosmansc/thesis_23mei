package thesis

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import financialFeatures._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.TableEnvironment

object FeatureCalculation {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val tableEnv = TableEnvironment.getTableEnvironment(env)
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


  def  calculation (stream: DataStream[StockQuotes]): DataStream[BolBandtypes] = {

    val SMA10 = SMA.calculateSMA(stream, tableEnv, env)

    val BB = BolBand.calculateBolBand(stream, tableEnv, env)
    BolBand.calculateBolBand(stream, tableEnv, env)




  }

}
