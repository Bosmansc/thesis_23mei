package thesis.financialFeatures

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes


case class SMAtypes(stockTime: Timestamp, stockName: String, SMA: Double)

object SMA {

  def calculateSMA(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[SMAtypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    val SMA10 = tableEnv.sqlQuery("SELECT stockTime, stockName ,  ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW),4) as SMA10" +
      "                           FROM stockTable" +
      "                           ")

    SMA10.toAppendStream[(SMAtypes)]

  }



}
