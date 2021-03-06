package thesis

import java.sql.Timestamp

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment


object Batch {
  //  def main(args: Array[String]) {

  // ***********
  // BATCH QUERY
  // ***********
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  // create a TableEnvironment
  val tableEnv = TableEnvironment.getTableEnvironment(bEnv)


  // ********************************************** read in data **********************************************
  // val csvInput = bEnv.readCsvFile[(Timestamp, String, Double, Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]("C:\\Users\\ceder\\Flink\\batch",
  val csvInput = bEnv.readCsvFile[(Timestamp, String, Double, Double, Int, Int, Int, Int, Int, Int, Int, Int, Int)]("C:\\Users\\ceder\\Flink\\GE_big")

  // make the label binary: if buy or sell -> label = 1 if hold -> label =-1
  tableEnv.registerDataSet("table1", csvInput)

  // the label (response variable) has to stand in the right row!
  val BaseTable = tableEnv.sqlQuery(
    """
                                      |SELECT   _5,_6,_7,_8,_9, _10,_11, _12, case when _13 = 2 OR _13 = 1 then 1 ELSE -1 END
                                      |FROM table1

                                    """.stripMargin)

  // convert the Table into a DataSet of Row
  val base: DataSet[(Int, Int, Int, Int, Int, Int, Int, Int, Int)] = tableEnv.toDataSet[(Int, Int, Int, Int, Int, Int, Int, Int, Int)](BaseTable)

  //  base.print()

  // labeling for not binary input:
  // labeling is correct, the order is just switched
  // labeling for binary input:
  val binaryLabeledInput = base
    .map { tuple =>
      val list = tuple.productIterator.toList
      val numList = list.map(_.asInstanceOf[Int].toDouble)
      LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
    }


  // labeledInput.print()

  // ********************************************** SVM **********************************************

  val trainTestDataSet = Splitter.trainTestSplit(binaryLabeledInput, 0.8, precise = true, seed = 100)
  val trainLV = trainTestDataSet.training
  val testLV = trainTestDataSet.testing

  val svm2 = SVM()

  svm2.setSeed(1)

  svm2.fit(trainLV)
  //  svm2.setOutputDecisionFunction(true)

  val testVD = testLV.map(lv => (lv.vector, lv.label))
  val evalSet = svm2.evaluate(testVD)

  // trainLV.print()
  // testVD.print()

  // groups the data in false negatives, false positives, true negatives, true positives
  evalSet.map(t => (t._1, t._2, 1)).groupBy(0, 1).reduce((x1, x2) => (x1._1, x1._2, x1._3 + x2._3)).print()
  val eval = evalSet.map(t => (t._1, t._2, 1)).groupBy(0, 1).reduce((x1, x2) => (x1._1, x1._2, x1._3 + x2._3))

  // save the model to later make predictions on streams:

  val modelSvm = svm2.weightsOption.get

  val weightVectorTypeInfo = TypeInformation.of(classOf[DenseVector])
  val weightVectorSerializer = weightVectorTypeInfo.createSerializer(new ExecutionConfig())
  val outputFormat = new TypeSerializerOutputFormat[DenseVector]
  outputFormat.setSerializer(weightVectorSerializer)

  //  modelSvm.write(outputFormat, "C:\\Users\\ceder\\Flink")

  //    bEnv.execute()

  // }
}