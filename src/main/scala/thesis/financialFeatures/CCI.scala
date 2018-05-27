package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class CCItypes(stockTime: Timestamp, stockName: String, typicalPrice: Double, numerator_CCI: Double, deviation: Double, MAD: Double, CCI: Double )

object CCI {

  def calculateCCI(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[CCItypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)


    // Commodity Channel Index, gewerkt met table -> stream -> table -> stream, mag dit?


    val CCI_1 = tableEnv.sqlQuery("SELECT stockTime, stockName, (high + low + lastPrice)/3 as typicalPrice," +
      "                            (high + low + lastPrice)/3 -  AVG((high + low + lastPrice)/3) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as numerator_CCI," +
      "                            " +

      "                          ABS( (high + low + lastPrice)/3 - AVG((high + low + lastPrice)/3) OVER(PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) ) as deviation" +
      "                           " +

      "                          FROM stockTable")

    val CCI_1_table = CCI_1.toAppendStream[( Timestamp,String, Double,Double, Double)]


    tableEnv.registerDataStream("CCI_1_table", CCI_1_table, 'stockTime, 'stockName, 'typicalPrice, 'numerator_CCI, 'deviation, 'UserActionTime.proctime )

    val CCI_2_table = tableEnv.sqlQuery("SELECT stockTime, stockName, typicalPrice,  numerator_CCI, deviation,   AVG(deviation)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as MAD, " +
      "                                 66.6666666667* ( numerator_CCI/(AVG(deviation)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) ) as CCI"  +
      "                                 FROM CCI_1_table")

    CCI_2_table.toAppendStream[(CCItypes)]




  }



}
