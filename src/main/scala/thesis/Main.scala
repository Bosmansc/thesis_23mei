package thesis

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment



object Main {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "stock")

    val stream: DataStream[StockQuotes] = env.addSource(new FlinkKafkaConsumer08[String]("stock", new SimpleStringSchema(), properties))
      .map(StockQuotes.fromString(_))

    // from which value a stock is considered to rise/fall: (recommended values: 0.01, 0.1, 0.2, 0.5, 0.75)
    val threshold = 0.1

    val test = FeatureCalculation.calculation(stream,threshold)
    test.print()






    env.execute()
  }
}
