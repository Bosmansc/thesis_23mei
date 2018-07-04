package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class WilliamRTypes(stockTime: Timestamp, stockName: String, willR_signal :Int )

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

    // lag table:
    tableEnv.registerDataStream("willR_lag", wilR_stream, 'stockTime, 'stockName, 'lastPrice,  'williamsR, 'UserActionTime.proctime )

    val willR_lag = tableEnv.sqlQuery("SELECT stockTime, stockName,  lastPrice, williamsR, SUM(williamsR) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - williamsR as williamsRLag" +
      "                             " +
      "                             FROM willR_lag ")


    val willR_lag_table = willR_lag.toAppendStream[(Timestamp, String, Double, Double, Double)]

    /*
     Buy if %R(t - 1) >= -80 and %R(t) < -80
     Sell if %R(t - 1) <= -20 and %R(t) > -20
     Hold otherwise
     1 = BUY, 2 = SELL, 0 = HOLD
      */

    tableEnv.registerDataStream("willR_big_table", willR_lag_table, 'stockTime, 'stockName, 'lastPrice,  'williamsR, 'williamsRLag, 'UserActionTime.proctime )

    //table to check outcome:
    val willR_signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, ROUND(williamsR,2), ROUND(williamsRLag,2)," +
      "                                       CASE WHEN williamsRLag >= -80 AND williamsR < -80 THEN 1 " +
      "                                       WHEN williamsRLag <= -20 AND williamsR > -20 THEN 2 ELSE 0 END as willR_signal" +
      "                                       FROM willR_big_table" +
      "                                       WHERE stockName = 'ABBV UN Equity'" +
      "                                        ")


    // signal: (14 iterations needed for useful results)
    val willR_signal = tableEnv.sqlQuery("SELECT stockTime, stockName," +
      "                                       CASE WHEN williamsRLag >= -80 AND williamsR < -80 THEN 1 " +
      "                                       WHEN williamsRLag <= -20 AND williamsR > -20  THEN 2 ELSE 0 END as willR_signal" +
      "                                       FROM willR_big_table" +
      //"                                       WHERE stockName = 'AAPL UW Equity'" +
      "                                        ")


    willR_signal.toAppendStream[(WilliamRTypes)]


  }



}
