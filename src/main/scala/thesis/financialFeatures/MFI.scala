package thesis.financialFeatures

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import thesis.StockQuotes



case class MFITypes(stockTime: Timestamp, stockName: String, lastPrice:Double, MFI_signal: Int)

object MFI {

  def calculateMFI(stream: DataStream[StockQuotes], tableEnv: TableEnvironment, env: StreamExecutionEnvironment): DataStream[MFITypes] = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    tableEnv.registerDataStream("stockTable", stream, 'stockName, 'stockTime , 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

    // Money Flow Indicator

    val mfi = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, (high + low + lastPrice)/3 as typicalPrice, ((high + low + lastPrice)/3)*volume as moneyFlow" +
      "                          FROM stockTable ")



    val mfi_tbl = mfi.toAppendStream[(Timestamp, String, Double,Double, Double)]

    tableEnv.registerDataStream("mfi_table_1", mfi_tbl, 'stockTime, 'stockName, 'lastPrice, 'typicalPrice, 'moneyFlow, 'UserActionTime.proctime )

    // negative of positive MF is defined by looking at the typical price rise or decline, not the MoneFlow!!
    val mfi_tbl_1 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +

      "                               CASE WHEN ROUND(2*typicalPrice - (SUM(typicalPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) > 0 THEN " +
      "                               ROUND( moneyFlow,6) ELSE 0 END as posMoneyFlow, " +

      "                               CASE WHEN ROUND(2*typicalPrice - (SUM(typicalPrice) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)),6) < 0 THEN " +
      "                               ROUND( moneyFlow  ,6) ELSE 0 END as negMoneyFlow" +

      "                               FROM mfi_table_1 ")

    val mfi_tbl_2 = mfi_tbl_1.toAppendStream[(Timestamp, String, Double, Double, Double)]

    tableEnv.registerDataStream("mfi_table_2", mfi_tbl_2, 'stockTime, 'stockName, 'lastPrice, 'posMoneyFlow, 'negMoneyFlow, 'UserActionTime.proctime )

    val mfi_tbl_3 = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, " +

      "                               ( AVG(posMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) )/" +
      "                               ( AVG(negMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) ) as moneyRatio," +

      "                               100 - 100/( 1 + ( (AVG(posMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) )/" +
      "                               ( AVG(negMoneyFlow) OVER (PARTITION BY stockName ORDER BY UserActionTime ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) ) ) )as moneyFlowIndex" +

      "                               FROM mfi_table_2 " )

    val mfi_stream = mfi_tbl_3.toAppendStream[(Timestamp, String, Double, Double, Double)]

    tableEnv.registerDataStream("mfi_signal", mfi_stream, 'stockTime, 'stockName, 'lastPrice, 'moneyRatio, 'moneyFlowIndex, 'UserActionTime.proctime )


    /*
    Buy if MFI (t) < 20
    Sell if MFI (t) > 80
    Hold otherwise
    1 = BUY, 2 = SELL, 0 = HOLD

    remark: work with direction (same as RSI) and very few 1 and 2

     */

    //table to check outcome:
    val mfi_signal_table = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice, moneyFlowIndex," +
      "                                       CASE WHEN moneyFlowIndex < 20 THEN 1 " +
      "                                       WHEN moneyFlowIndex > 80  THEN 2 ELSE 0 END as MFI_signal" +
      "                                       FROM mfi_signal" +
      "                                        ")

    // signal: (with lastPrice to use in baseTable)
    val mfi_signal = tableEnv.sqlQuery("SELECT stockTime, stockName, lastPrice," +
      "                                       CASE WHEN moneyFlowIndex < 20 THEN 1 " +
      "                                       WHEN moneyFlowIndex > 80  THEN 2 ELSE 0 END as MFI_signal" +
      "                                       FROM mfi_signal" )


    mfi_signal.toAppendStream[(MFITypes)]

  }



}
