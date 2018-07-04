package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class BolBandtypes(stockTime: Timestamp, stockName: String,  lastPriceLag: Double, BB_signal:Int )

object BolBand {

  def calculateBolBand(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[BolBandtypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    // Financial measure Bollinger Bands,

    val BB = tableEnv.sqlQuery("SELECT stockTime, stockName ,lastPrice, ROUND(AVG(lastPrice) " +
      "                           OVER ( PARTITION BY stockName" +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),3) as BB_middleBand," +

      "                           ROUND( AVG(lastPrice)" +
      "                           OVER ( PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),3) + " +
      "                           2*(STDDEV_SAMP(lastPrice) OVER (PARTITION BY stockName " +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)) as BB_upperBound," +

      "                           ROUND( AVG(lastPrice)" +
      "                           OVER ( PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),3) - " +
      "                           2*(STDDEV_SAMP(lastPrice) OVER (PARTITION BY stockName " +
      "                           ORDER BY UserActionTime" +
      "                           ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)) as BB_lowerBound" +

      "                           FROM stockTable" +
      "                           ")

    val bol_band_table = BB.toAppendStream[( Timestamp,String, Double,Double, Double, Double)]

    tableEnv.registerDataStream("bol_band_table", bol_band_table, 'stockTime, 'stockName, 'lastPrice, 'BB_middleBand, 'BB_upperBound, 'BB_lowerBound, 'UserActionTime.proctime )

    // if lastPrice from previous  minute is higher or equal to the BB-lower bound from prev minute
    // and the current lastPrice is lower than the upperbound than SELL

    // if the lastPrice from the prev min is lower or equal to the BB-higher bound from prev min
    // and the current lastPrice is higher than the upperbound than BUY

    // otherwise hold

    /*
    Buy if close (t - 1) <= UBB (t -  1) and close (t) > UBB (t)
    Sell if close (t - 1) >= LBB (t - 1) and close (t) < LBB (t)
    Hold otherwise

    1 = BUY, 2 = SELL, 0 = HOLD
     */

    // USE CASE to get rid of Nan

    val BB_Nan = tableEnv.sqlQuery("SELECT stockTime, stockName , lastPrice,  " +
      "                                 case when BB_middleBand > 0 OR BB_middleBand < 0 OR BB_middleBand =0 then BB_middleBand else 0 end ," +
      "                                 case when BB_upperBound >= 0 OR BB_upperBound < 0  then BB_upperBound else 0 end ," +
      "                                 case when BB_lowerBound >= 0 OR BB_lowerBound < 0  then BB_lowerBound else 0 end " +
      "                                 "  +
      "                                 FROM bol_band_table" +
      "                                 ")

    val BBNan = BB_Nan.toAppendStream[( Timestamp,String, Double,Double, Double, Double)]

    tableEnv.registerDataStream("BBNan", BBNan, 'stockTime, 'stockName, 'lastPrice, 'BB_middleBand, 'BB_upperBound, 'BB_lowerBound, 'UserActionTime.proctime )



    val lag_table = tableEnv.sqlQuery("SELECT stockTime, stockName , lastPrice, BB_middleBand, BB_upperBound, BB_lowerBound," +

      "                               SUM(lastPrice)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - lastPrice  as lastPriceLag," +

      "                               SUM(BB_upperBound)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - BB_upperBound  as BB_upperBoundLag," +

      "                               SUM(BB_lowerBound)OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - BB_lowerBound  as BB_lowerBoundLag" +

      "                               FROM  BBNan ")

    val lagTable = lag_table.toAppendStream[( Timestamp,String, Double,Double, Double, Double, Double, Double, Double)]

    tableEnv.registerDataStream("bol_band_table_signal", lagTable, 'stockTime, 'stockName, 'lastPrice, 'BB_middleBand, 'BB_upperBound, 'BB_lowerBound, 'lastPriceLag , 'BB_upperBoundLag, 'BB_lowerBoundLag, 'UserActionTime.proctime )


    /*
   Buy if close (t - 1) <= UBB (t -  1) and close (t) > UBB (t)
   Sell if close (t - 1) >= LBB (t - 1) and close (t) < LBB (t)
   Hold otherwise

   1 = BUY, 2 = SELL, 0 = HOLD
    */

    // table to check the outcome:

    val BB_signal_table_big = tableEnv.sqlQuery("SELECT stockTime, stockName , lastPrice, ROUND(BB_middleBand,2), ROUND(BB_upperBound,2), ROUND(BB_lowerBound,2), ROUND(lastPriceLag,2) , ROUND(BB_upperBoundLag,2), ROUND(BB_lowerBoundLag,2)," +
      "                                       CASE WHEN lastPriceLag <= BB_upperBoundLag AND lastPrice > BB_upperBound  THEN 1 " +
      "                                       WHEN lastPriceLag >= BB_lowerBoundLag AND lastPrice < BB_lowerBound THEN 2 ELSE 0 END as BB_signal  " +

      "                                       FROM  bol_band_table_signal" +
      "                                       WHERE stockName = 'ABT UN Equity' ")

    // signal: (lastPriceLag included for response variable calculation in FeatureCalculation)
    val BB_signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPriceLag," +
      "                                       CASE WHEN lastPriceLag <= BB_upperBoundLag AND lastPrice > BB_upperBound  THEN 1 " +
      "                                       WHEN lastPriceLag >= BB_lowerBoundLag AND lastPrice < BB_lowerBound THEN 2 ELSE 0 END as BB_signal  " +

      "                                       FROM  bol_band_table_signal" )



    BB_signal_table.toAppendStream[(BolBandtypes)]

  }



}
