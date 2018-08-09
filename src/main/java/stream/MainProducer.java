package stream;

import java.io.*;
import java.text.*;
import org.apache.commons.compress.archivers.dump.*;


public class MainProducer {

    public static void main(String[] args) throws IOException, ParseException, InvalidFormatException, InterruptedException {

        // String FILE_NAME = "OriginallData";
        //  String FILE_NAME = "SmallData";
        //  String FILE_NAME = "AAPL_big";
        //  String FILE_NAME = "JNJ_big";
        //  String FILE_NAME = "GE_big";
        //  String FILE_NAME = "MS_big";
        String FILE_NAME = "C:\\Users\\ceder\\Documents\\Thesis\\stock_data\\C_big";

        StockExchange stockExchange = new StockExchange();

        stockExchange.readAllCsv(FILE_NAME);

        stockExchange.SendData();
    }
}
