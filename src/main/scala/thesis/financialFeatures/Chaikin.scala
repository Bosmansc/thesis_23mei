package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class ChaikinTypes(stockTime: Timestamp, stockName: String, lastPrice:Double, moneyFlowMultiplier: Double, moneyFlowVolume: Double, chaikinMoneyFlow:Double )

object Chaikin {

  def calculateChaikin(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[ChaikinTypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    // Chaikin Accumulation/Distribution

    //   [(Close  -  Low) - (High - Close)] /(High - Low) = Money Flow Multiplier


    val chai = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, volume, ( (lastPrice - low) - (high - lastPrice) )/( high - low) as moneyFlowMultiplier," +
      "                           CASE WHEN (high-low) > 0 THEN ( ( (lastPrice - low) - (high - lastPrice) )/( high - low) ) * volume ELSE 0 END as moneyFlowVolume" +
      "                            FROM stockTable ")

    val chai_tbl = chai.toAppendStream[(Timestamp, String, Double,Double, Double, Double)]



    tableEnv.registerDataStream("chai_tbl_1", chai_tbl, 'stockTime, 'stockName, 'lastPrice, 'volume, 'moneyFlowMultiplier, 'moneyFlowVolume, 'UserActionTime.proctime )

    // Round hier nog fixen
    val chai_table_2 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, moneyFlowMultiplier, moneyFlowVolume, " +

      "                                  ( SUM(moneyFlowVolume) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) )/" +
      "                                  ( SUM(volume) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) ) as chaikinMoneyFlow" +
      "                                   FROM chai_tbl_1 " +
      "                                   WHERE stockName = 'ABT UN Equity'")

    val chai_stream = chai_table_2.toAppendStream[(Timestamp, String, Double, Double, Double, Double)]
    chai_table_2.toAppendStream[(ChaikinTypes)]


  }



}
