package thesis

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment


object Batch {
  def main(args: Array[String]) {

    // ***********
    // BATCH QUERY
    // ***********
    val bEnv = ExecutionEnvironment.getExecutionEnvironment
    // create a TableEnvironment for batch queries
    val bTableEnv = TableEnvironment.getTableEnvironment(bEnv)

   // val csvInput = bEnv.readCsvFile[(Timestamp, String, Double, Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]("C:\\Users\\ceder\\Flink\\batch",
    val csvInput = bEnv.readCsvFile[(Int, Int, Int, Int, Int, Int, Int, Int, Int)]("C:\\Users\\ceder\\Flink\\batch",
      includedFields = Array(5,7,9,11,13,15,17,19,20))

    val csvInputTest = bEnv.readCsvFile[(Int, Int, Int, Int, Int, Int, Int, Int)]("C:\\Users\\ceder\\Flink\\batch",
      includedFields = Array(5,7,9,11,13,15,17,19))

    // labeling is correct, the order is just switched, problem: the numbers are changed to double! and not Int
    val labeledInput = csvInput
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
      }

    // labeledVector input without repons variable
    val labeledInputTest = csvInput
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        DenseVector(numList.take(8).toArray)
      }

    // vector input without respons variable
    val inputVectorTest = csvInput
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        Vector(numList.take(8).toArray)
      }

    val astroTest =  labeledInput
      .map(x => (x.vector, x.label))

    // SVM model:
    val svm = SVM()
      .setBlocks(bEnv.getParallelism)
      .setIterations(100)
      .setRegularization(0.001)
      .setStepsize(0.1)
      .setSeed(42)

    svm.fit(labeledInput)

    svm.predict(labeledInputTest)

    val evaluationPairs: DataSet[(Double, Double)] = svm.evaluate(astroTest)
    // evaluationPairs.print()


    // Create multiple linear regression learner
    val mlr = MultipleLinearRegression()
      .setIterations(10)
      .setStepsize(0.5)
      .setConvergenceThreshold(0.001)

    // Obtain training and testing data set
    val trainingDS: DataSet[LabeledVector] = labeledInput
    val testingDS = labeledInputTest

    // Fit the linear model to the provided data
    mlr.fit(trainingDS)

    // Calculate the predictions for the test data
    val predictions = mlr.predict(testingDS)

    predictions.print()


    /*
    // create model
    val als = ALS()
          .setIterations(10)
          .setNumFactors(10).setBlocks(100).setTemporaryPath("C:\\Users\\ceder\\Flink\\als")

    val parameters = ParameterMap().add(ALS.Lambda,0.9) // lambda is regularization parameter -> so the model does not overfit
      .add(ALS.Seed, 42L) // ALS starts with random feed and start is provided here

    als.fit(csvInput, parameters)

    val predictions = als.predict(csvInputTest)

    predictions.print()

    */

    bEnv.execute()

  }
}

