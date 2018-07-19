package thesis

import java.util.Properties

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sinks.CsvTableSink



object Main {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //Configure Flink to perform a consistent checkpoint of a program’s operator state every 1000ms.
    env.enableCheckpointing(1000)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "stock16")

    val stream: DataStream[StockQuotes] = env.addSource(new FlinkKafkaConsumer08[String]("stock16", new SimpleStringSchema(), properties))
      .map(StockQuotes.fromString(_))


    // from which value a stock is considered to rise/fall: (recommended values: 0.01, 0.1, 0.2, 0.5, 0.75)
    val threshold = 0.1

    val test = FeatureCalculation.calculation(stream, threshold)

    // convert stream to dataSet (stream to batch to make predictions)
    val table1: Table = tableEnv.fromDataStream(test)

    test.print()

    // nog loop maken die na aantal sec stopt, https://stackoverflow.com/questions/18358212/scala-looping-for-certain-duration werkt niet

    // work with if: if return is to low: then write to sink
    table1.writeToSink(
      new CsvTableSink(
        "C:\\Users\\ceder\\Flink\\GE_big", // output path
        fieldDelim = ",", // optional: delimit files by '|'
        numFiles = 1, // optional: write to a single file
        writeMode = WriteMode.NO_OVERWRITE)) // optional: override existing files

    // val model = Batch.modelSvm


    //  val prediction = input.map(new Predictor[_])
//    Batch.modelSvm


    env.execute()


  }
}