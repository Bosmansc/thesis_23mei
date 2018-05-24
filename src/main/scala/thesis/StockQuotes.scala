package thesis


object StockQuotes {
  def fromString(s: String): StockQuotes = {
    val parts = s.split(",")

    val DateTime = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm")

    new StockQuotes(
      parts(0),
      //    DateTime.parse(parts(1)),
      parts(1).replace('T', ' ').concat(":00.000"),
      parts(2).toDouble,
      parts(3).toDouble,
      parts(4).toDouble,
      parts(5).toDouble,
      parts(6).toDouble,
      parts(7).toDouble

    )

  }

}

case class StockQuotes(stockName: String, stockTime: String, open: Double, high: Double, low: Double, lastPrice: Double, number: Double, volume: Double){

}
