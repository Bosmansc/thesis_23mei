package thesis

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.regression.MultipleLinearRegression
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment


object Batch {

    // ***********
    // BATCH QUERY
    // ***********
    val bEnv = ExecutionEnvironment.getExecutionEnvironment
    // create a TableEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(bEnv)

    // ********************************************** read in data **********************************************
    // val csvInput = bEnv.readCsvFile[(Timestamp, String, Double, Double, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, String, Int, Int)]("C:\\Users\\ceder\\Flink\\batch",
    val csvInput = bEnv.readCsvFile[(Int, Int, Int, Int, Int, Int, Int, Int, Int)]("C:\\Users\\ceder\\Flink\\XOMbig",
      includedFields = Array(5,7,9,11,13,15,17,19,20))

    val csvInputTest = bEnv.readCsvFile[(Int, Int, Int, Int, Int, Int, Int, Int)]("C:\\Users\\ceder\\Flink\\batch",
      includedFields = Array(5,7,9,11,13,15,17,19))

    // make the label binary: if buy or sell -> label = 1 if hold -> label =-1
    tableEnv.registerDataSet("table1", csvInput )
    val BaseTable = tableEnv.sqlQuery("""
                                      |SELECT _1, _2, _3,_4,_5,_6,_7,_8, case when _9 = 2 OR _9 = 1 then 1 ELSE -1 END
                                      |FROM table1

                                    """.stripMargin)

    // convert the Table into a DataSet of Row
    val base: DataSet[(Int, Int, Int, Int, Int, Int, Int, Int, Int)] = tableEnv.toDataSet[(Int, Int, Int, Int, Int, Int, Int, Int, Int)](BaseTable)

    // labeling for not binary input:
    // labeling is correct, the order is just switched, problem: the numbers are changed to double! and not Int
    val labeledInput = csvInput
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
      }


    // labeling for binary input:
    val binaryLabeledInput = base
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        LabeledVector(numList(8), DenseVector(numList.take(8).toArray))
      }


    // labeledVector input without reponse variable
    val labeledInputTest = csvInput
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        DenseVector(numList.take(8).toArray)
      }


    // vector input without response variable
    val inputVectorTest = csvInput
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        Vector(numList.take(8).toArray)
      }


    val astroTest =  labeledInput
      .map(x => (x.vector, x.label))

   // labeledInput.print()

    // ********************************************** SVM **********************************************
    // SVM model:

    val svm = SVM()
      .setBlocks(bEnv.getParallelism)
      .setIterations(100)
      .setRegularization(0.001)
      .setStepsize(0.1)
      .setSeed(42)

    svm.fit(labeledInput)

    svm.predict(labeledInputTest)

    val predictionDS: DataSet[(DenseVector, Double)] = svm.predict(labeledInputTest)




    val evaluationPairs: DataSet[(Double, Double)] = svm.evaluate(astroTest)
    // evaluationPairs.print()

  // svm.evaluate(predictionDS).print()


    // ********************************************** SVM example 2 **********************************************
  //  val input: DataSet[(Int, Int, Boolean, Double, Double, Double)] = env.readCsvFile(filepathTraining, ignoreFirstLine = true, fieldDelimiter = ";")

  /*  val inputLV = input.map(
      t => { LabeledVector({if(t._3) 1.0 else -1.0}, DenseVector(Array(t._4, t._5, t._6)))}
    )*/

    val trainTestDataSet = Splitter.trainTestSplit(binaryLabeledInput, 0.8, precise = true, seed = 100)
    val trainLV = trainTestDataSet.training
    val testLV = trainTestDataSet.testing

    val svm2 = SVM()

    svm2.fit(trainLV)
  //  svm2.setOutputDecisionFunction(true)

    val testVD = testLV.map(lv => (lv.vector, lv.label))
    val evalSet = svm2.evaluate(testVD)

   // trainLV.print()
  //  testVD.print()


    // groups the data in false negatives, false positives, true negatives, true positives
    evalSet.map(t => (t._1, t._2, 1)).groupBy(0,1).reduce((x1,x2) => (x1._1, x1._2, x1._3 + x2._3)).print()

    // get the weights of SVM:
   // val weights = svm.weightsOption.get.collect()


    // save the model to later make predictions on streams:

    val modelSvm = svm2.weightsOption.get



    svm2.weightsOption.get.print()

    val weightVectorTypeInfo = TypeInformation.of(classOf[DenseVector])
    val weightVectorSerializer = weightVectorTypeInfo.createSerializer(new ExecutionConfig())
    val outputFormat = new TypeSerializerOutputFormat[DenseVector]
    outputFormat.setSerializer(weightVectorSerializer)

  //  modelSvm.write(outputFormat, "C:\\Users\\ceder\\Flink")

    val svm3 = SVM()

    svm3.weightsOption = Option(modelSvm)
    svm3.weightsOption.get.print()

    svm3.fit(trainLV)
    //  svm2.setOutputDecisionFunction(true)

    val testVD2 = testLV.map(lv => (lv.vector, lv.label))
    val evalSet2 = svm2.evaluate(testVD2)

    // trainLV.print()
    //  testVD.print()


    // groups the data in false negatives, false positives, true negatives, true positives
    evalSet2.map(t => (t._1, t._2, 1)).groupBy(0,1).reduce((x1,x2) => (x1._1, x1._2, x1._3 + x2._3)).print()



    // safe the model to a file so it is accessible for the streaming program





/*
    val svmDenseVector = baseSvm
      .map{tuple =>
        val list = tuple.productIterator.toList
        val numList = list.map(_.asInstanceOf[Int].toDouble)
        DenseVector(numList.take(8).toArray)
      }


    val svm4 = SVM()

    svm4.weightsOption = Option(modelSvm)
    svm4.weightsOption.get.print()

    svm4.fit(trainLV)
    //  svm2.setOutputDecisionFunction(true)

    val testVD3 = testLV.map(lv => (lv.vector, lv.label))
    val evalSet3 = svm4.evaluate(testVD3)

    // trainLV.print()
    //  testVD.print()


    // groups the data in false negatives, false positives, true negatives, true positives
    evalSet3.map(t => (t._1, t._2, 1)).groupBy(0,1).reduce((x1,x2) => (x1._1, x1._2, x1._3 + x2._3)).print()

*/




    // ********************************************** multiple linear regression **********************************************
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

   // predictions.print()


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
