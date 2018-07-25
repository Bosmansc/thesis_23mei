
package thesis

import java.util.Properties

import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
// This will be all that you need
import io.radicalbit.flink.pmml.scala.api.reader.ModelReader
import org.apache.flink.streaming.api.scala._



object Jpmml {

  val kafkaName = "stock28" // to link kafka with stream producer
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //Configure Flink to perform a consistent checkpoint of a program’s operator state every 1000ms.
    env.enableCheckpointing(1000)

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", s"$kafkaName")

    val stream: DataStream[StockQuotes] = env.addSource(new FlinkKafkaConsumer08[String](s"$kafkaName", new SimpleStringSchema(), properties))
      .map(StockQuotes.fromString(_))


    // The difference between the next closing price when is stockQuote is considered to increase/decrase (recommended values: 0.01, 0.1, 0.2, 0.5, 0.75)
    val threshold = 0.05


    // **************************** STREAMING model: ****************************

    val streamWithFeatures = FeatureCalculation.calculation(stream)

    //Load PMML model
    val pathToPmml = "C:\\Users\\ceder\\Flink"
    val pathToPmml2 = "C:\\Users\\ceder\\Flink\\svm.pmml"

    //  a lazy reader implementation
    val reader = ModelReader(pathToPmml2)



    //Convert iris to DenseVector
  //  val irisToVector = irisDataStream.map(iris => iris.toVector)
  //  val steamToVector = streamWithFeatures.map( x => x.toVector)

    val labeledInputTest = streamWithFeatures
      .map { tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        DenseVector(numList.take(8).toArray)
      }

    labeledInputTest.print()


    //Load PMML model
    val model = ModelReader(pathToPmml2)



    print(model)








  }
}




















