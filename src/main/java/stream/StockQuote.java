package stream;

import java.time.*;
import java.util.*;

public class StockQuote {

    private LocalDateTime date;
    private double open;
    private double high;
    private double low;
    private double lastPrice;
    private double number;
    private double volume;


    public StockQuote(LocalDateTime date, double open, double high, double low, double lastPrice, double number, double volume) {
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.lastPrice = lastPrice;
        this.number = number;
        this.volume = volume;
    }

    public StockQuote(LocalDate date, ArrayList stockInfo) {

        this.date = LocalDateTime.parse(date.toString());
        this.open = ((Double) stockInfo.get(0)).doubleValue();
        this.high = ((Double) stockInfo.get(1)).doubleValue();

        this.low = ((Double) stockInfo.get(2)).doubleValue();

        this.lastPrice = ((Double) stockInfo.get(3)).doubleValue();

        this.number = ((Double) stockInfo.get(4)).doubleValue();

        this.volume = ((Double) stockInfo.get(5)).doubleValue();


    }


    @Override
    public String toString() {
        return date + ", " + open + ", " + high +
                ", " + low + ", " + lastPrice + ", " + number + ", " + volume + '\n';
    }

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getLastPrice() {
        return lastPrice;
    }

    public void setLastPrice(double lastPrice) {
        this.lastPrice = lastPrice;
    }

    public double getNumber() {
        return number;
    }

    public void setNumber(double number) {
        this.number = number;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }
}
