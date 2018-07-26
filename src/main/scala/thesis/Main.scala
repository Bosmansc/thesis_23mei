package thesis

import java.sql.Timestamp
import java.util.Properties

import io.radicalbit.flink.pmml.scala._
import io.radicalbit.flink.pmml.scala.api.reader.ModelReader
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink


object Main {

  val kafkaName = "stock37" // to link kafka with stream producer
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //Configure Flink to perform a consistent checkpoint of a program’s operator state every 1000ms.
    env.enableCheckpointing(100000)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", s"$kafkaName")

    val stream: DataStream[StockQuotes] = env.addSource(new FlinkKafkaConsumer08[String](s"$kafkaName", new SimpleStringSchema(), properties))
      .map(StockQuotes.fromString(_))

    // The difference between the next closing price when is stockQuote is considered to increase/decrase (recommended values: 0.01, 0.1, 0.2, 0.5, 0.75)
    val threshold = 0.05

    // true or false to generate batch data, to generate extern model
    val batchModel = false
    // true or false to make predictions
    val predictionModel = true


    // ******************************************************* STREAMING model: *******************************************************

    val streamWithFeatures = FeatureCalculation.calculation(stream)

    //  streamWithFeatures.print()

    // ******************************************************* PREDICTION model: *******************************************************
    if (predictionModel) {

      // print lastPrice (just a visual)
      val lastPriceStream = stream.map(_.lastPrice)
      lastPriceStream.print()

      tableEnv.registerDataStream("stockTableTime", stream, 'stockName, 'stockTime, 'priceOpen, 'high, 'low, 'lastPrice, 'number, 'volume, 'UserActionTime.proctime)

      val streamTimeTable = tableEnv.sqlQuery("SELECT lastPrice, UserActionTime " +
        "                                    FROM stockTableTime")

      val streamTime = streamTimeTable.toAppendStream[(Double, Timestamp)]
      streamTime.print()

      //Load PMML model
      val pathToPmml2 = "C:\\Users\\ceder\\Flink\\BatchStockData\\rf_AAPL.pmml"

      //Load PMML model
      val modelReader = ModelReader(pathToPmml2)

      // *****  Using evaluate operator

      val prediction = streamWithFeatures.evaluate(modelReader) {

        case (event, model) =>
          val vectorized = event.toVector
          val prediction = model.predict(vectorized, Some(0.0))
          (event, prediction.value.getOrElse(-1.0))
      }

      tableEnv.registerDataStream("predictionTable", prediction, 'SMA_signal, 'prediction, 'UserActionTime.proctime)

      val predictionTable = tableEnv.sqlQuery("SELECT prediction, UserActionTime " +
        "                                    FROM predictionTable")

      val predictionStream = predictionTable.toAppendStream[(Double, Timestamp)]
      predictionStream.print()


    } else {print("no prediction model is generated")}

    // ******************************************************* BATCH model: *******************************************************
    if(batchModel) {
      val batchStreamWithFeatures = FeatureCalculationBatch.calculation(stream, threshold)

      // convert stream to dataSet (stream to batch to make predictions)
      val batchTable: Table = tableEnv.fromDataStream(batchStreamWithFeatures)

      //define output location name:
      val outputLocation = "XOM_big_bigger2"



      // work with if: if return is to low: then write to sink
      batchTable.writeToSink(
        new CsvTableSink(
          s"C:\\Users\\ceder\\Flink\\BatchStockData\\$outputLocation", // output path
          fieldDelim = ",", // optional: delimit files by '|'
          numFiles = 1, // optional: write to a single file
          writeMode = WriteMode.NO_OVERWRITE)) // optional: override existing files

    } else {print("no batch data is generated")}


    env.execute()


  }
}