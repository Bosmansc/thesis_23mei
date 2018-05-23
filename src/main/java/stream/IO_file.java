package stream;

import java.io.*;
import java.time.*;
import java.time.format.*;

public class IO_file {

    public static Stock readStockData(File f) throws IOException {
        BufferedReader br = null;
        String stockName = f.getName();

        Stock stock = new Stock(stockName);

        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            StockQuote stockQuote;

            while ((line = br.readLine()) != null) {
                stockQuote = processLine(line);
                if (stockQuote != null) stock.addStockQuote(stockQuote);
                // System.out.println(stockQuote);
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }

        return stock;
    }

    private static StockQuote processLine(String line) {
        String[] info = line.split(",");

        if (info.length != 7) return null;
        if (info[0].contains("NAME")) return null;

        try {

            DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

        LocalDateTime date = LocalDateTime.parse(info[0].toString(), df);
        Double open = Double.parseDouble(info[1]);
        Double high = Double.parseDouble(info[2]);
        Double low = Double.parseDouble(info[3]);
        Double lastPrice = Double.parseDouble(info[4]);
        Double number = Double.parseDouble(info[5]);
        Double volume = Double.parseDouble(info[6]);

            return new StockQuote(date, open, high, low, lastPrice, number, volume);

        }
        catch (Exception e) {return null;}


    }

}
