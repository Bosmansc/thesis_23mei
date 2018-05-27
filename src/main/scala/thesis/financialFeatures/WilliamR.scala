package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class WilliamRTypes(stockTime: Timestamp, stockName: String, lastPrice:Double, williamsR: Double )

object WilliamR {

  def calculateWilliamR(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[WilliamRTypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)


    // William' %R

    // %R = (Highest High - Close)/(Highest High - Lowest Low) * -100

    val wilR = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +
      "                           -100 * ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) - lastPrice )/" +
      "                           ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ) as williamsR " +
      "                           FROM stockTable")

    val wilR_stream = wilR.toAppendStream[(Timestamp, String, Double,Double)]
    wilR.toAppendStream[(WilliamRTypes)]


  }



}
