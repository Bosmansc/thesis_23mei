package stream;

import java.io.*;
import java.text.*;
import java.time.*;
import java.util.*;
import org.apache.commons.compress.archivers.dump.*;
import org.apache.kafka.clients.producer.*;
import thesis.*;


public class StockExchange {

    public ArrayList<Stock> stocks;


    public StockExchange() {

        this.stocks = new ArrayList<>();
    }

    public void readAllCsv(String rootMap) throws IOException, ParseException, InvalidFormatException {

        Stock s;

        File file = new File(rootMap);
        System.out.println("Data dir : " + file.getAbsolutePath());
        File[] files = file.listFiles();

        for (File f : files) {


                    if (f.isFile()) {
                        try {
                            s = IO_file.readStockData(f);

                            this.stocks.add(s);

                        } catch(IOException e) {
                            System.err.println("ERROR");
                        }
                    }
        }

        }


    public void SendData() throws InterruptedException {

        // kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // initialises delay between minutes:
        int lag = 1000;

        StockQuote stockQuote;

        ArrayList<StockQuote> stockQuotes = stocks.get(0).getStockQuotes();

        //    LocalDateTime start = LocalDateTime.of(2015, 4, 27, 15, 31);
        LocalDateTime start = stockQuotes.get(0).getDate();

        //   LocalDateTime end = LocalDateTime.of(2015, 11, 6, 21, 59);
        LocalDateTime end = stockQuotes.get(stockQuotes.size() - 1).getDate();

        String name = Main.kafkaName();

        for (LocalDateTime date = start; date.isBefore(end); date = date.plusMinutes(1)) {

            for (Stock s : stocks) {

                for (int i = 0; i < s.getStockQuotes().size(); i++) {

                    stockQuote = s.getStockQuotes().get(i);

                    if (stockQuote.getDate().equals(date)) {

                        System.out.println(s.getStockName() + ", " + stockQuote);

                        producer.send(new ProducerRecord<String, String>(name, s.getStockName() + ", " + stockQuote));

                        s.getStockQuotes().remove(stockQuote);
                    }

                }
            }

            Thread.sleep(lag);

        }


    }

    @Override
    public String toString() {
        return "StockExchange{" +
                "stocks=" + stocks +
                '}';
    }
}






