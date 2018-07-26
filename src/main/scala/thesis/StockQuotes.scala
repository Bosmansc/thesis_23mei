package thesis

import java.sql.Timestamp
import java.text.SimpleDateFormat

case class StockQuotes(stockName: String, stockTime: Timestamp, priceOpen: Double, high: Double, low: Double, lastPrice: Double, number: Double, volume: Double)

object StockQuotes {

  def getTimestamp(s: String): Timestamp = {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
    new Timestamp(format.parse(s).getTime)

  }

  def fromString(s: String): StockQuotes = {
    val parts = s.split(",")

    //  val DateTime = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm")


    new StockQuotes(
      parts(0),
      // DateTime.parse(parts(1)),
      // parts(1).replace('T', ' ').concat(":00.000"),
      getTimestamp(parts(1)),
      parts(2).toDouble,
      parts(3).toDouble,
      parts(4).toDouble,
      parts(5).toDouble,
      parts(6).toDouble,
      parts(7).toDouble

    )

  }

}

