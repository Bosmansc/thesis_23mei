package thesis

object StockFeatures {
  def fromString(s: String): StockFeatures = {
    val parts = s.split(",")

      new StockFeatures(
        parts(0),
        parts(1),
        parts(2).toDouble,
        parts(3).toDouble,
        parts(4).toDouble,
        parts(5).toDouble,
        parts(6).toDouble,
        parts(7).toDouble

      )

  }

}

case class StockFeatures(stockName: String, stockTime: String, open: Double, high: Double, low: Double, lastPrice: Double, number: Double, volume: Double){

}
