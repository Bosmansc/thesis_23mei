package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class StochTypes(stockTime: Timestamp, stockName: String, lastPrice:Double, K: Double, D: Double )

object Stoch {

  def calculateStoch(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[StochTypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)


    // Stochastic Oscillator

    val stoch = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, 100*((( lastPrice - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) )/" +
      "                           ( MAX(high) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) - MIN(low) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) ))) as K " +
      "                           FROM stockTable ")

    val stoch_table = stoch.toAppendStream[( Timestamp,String, Double,Double)]

    tableEnv.registerDataStream("stoch_table_2", stoch_table, 'stockTime, 'stockName, 'lastPrice,'K, 'UserActionTime.proctime )

    val stoch_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, K, ( AVG(K)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) ) as D "  +
      "                                    FROM stoch_table_2" )

    stoch_table_2.toAppendStream[(StochTypes)]

  }



}
