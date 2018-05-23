package stream;

import java.util.*;

public class Stock {

    private String stockName;

    private ArrayList<StockQuote> stockQuotes;

    public Stock(String stockName) {

        this.stockName = stockName.substring(0, stockName.length() - 4);
        this.stockQuotes = new ArrayList<>();
    }

    public String getStockName() {
        return stockName;
    }

    public ArrayList<StockQuote> getStockQuotes() {
        return stockQuotes;
    }

    public void addStockQuote (StockQuote sq){

        this.stockQuotes.add(sq);
    }

    @Override
    public String toString() {
        return "Stock{" +
                "stockName='" + stockName + '\'' +
                ", stockQuotes=" + stockQuotes + stockName +
                '}';
    }

}
