package bdproc.spark_ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import bdproc.common.Utilities._
import org.apache.spark.sql.types._

import scala.collection.mutable

object PredicitiveModelPrizeML {

  def main(args: Array[String]): Unit = {
    //session spark
    val spark = SparkSession
      .builder
      .appName("Regression ML")
      .master("local[*]")
      .config("spark.executor.memory","2g")
      .getOrCreate()

    import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))


    val esquema = new StructType().add("LocationID", IntegerType, true)
      .add("Bedrooms", IntegerType, true).add("Bathrooms", IntegerType, true)
      .add("Size", IntegerType, true).add("Price", IntegerType, true)

    val rawDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header",false)
      .option("delimiter",",")
      .schema(esquema)
      .load(pathToRealStateFileML)

    val rdd = rawDF.rdd.zipWithUniqueId()

    //MAP o diccionario (local) con los valores de calidad de los datos iniciales
    val lookupQuality = rdd.map{ case (r: Row, id: Long)=> (id, ( r.getInt(0), r.getInt(3), r.getInt(4)) )}
      .collect().toMap

    //crear un conjunto de features
    val d = rdd.map{case (r: Row, id: Long)
    =>
      {
        LabeledPoint(id, Vectors.dense(r.getInt(0),r.getInt(1), r.getInt(2), r.getInt(3)))
      } }.cache()


    //conjuntos de entrenamiento y test
    val trainQ = new mutable.Queue[RDD[LabeledPoint]]()
    val testQ = new mutable.Queue[RDD[LabeledPoint]]()

    //creacion de modelo
    val trainingStream = ssc.queueStream(trainQ)
    val testStream = ssc.queueStream(testQ)

    //rellenamos las colas
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(4)) //4 es num. features
      .setNumIterations(25)
      .setStepSize(1e-2)
      .setMiniBatchFraction(0.25)
        .setRegParam(0.1)
        .setConvergenceTol(1e-4)

    //entrenar modelo
    model.trainOn(trainingStream)
    val result = model.predictOnValues(testStream.map(lp => (lp.label, lp.features)))

    result.map{ case (id: Double, prediction: Double) =>
      (id, prediction, lookupQuality(id.asInstanceOf[Long])) }
      .print()

    ssc.start


    //reparticion d datos entre conjunto de entrenamiento (80%) y de validacion (20%)
    val Array(trainData, test) = d.randomSplit(Array(.80, .20))

    trainQ +=  trainData
    Thread.sleep(4000) //esperamos cuatro segundos

    //reparticion d datos de test en dos conjuntos iguales (al 50%)
    val testGroups = test.randomSplit(Array(.50, .50))
    testGroups.foreach(group => {
      testQ += group

      Thread.sleep(2000) //esperamos dos segundos
    })



    ssc.stop()
  }
}
