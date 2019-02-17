package bdproc.practica_alberto

import net.liftweb.json._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import bdproc.common.Utilities._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.mutable
import scala.util.Random

object SparkProductoInmobiliario
{

  var incrementalCode = 10000
  val dictionary = new mutable.HashMap[String,Integer]()
  //link : https://www.google.com/search?q=convertir+pies+cuadrados+a+metros+cuadrados&oq=convertir+pies+cuadrados+a+metros+cuadr&aqs=chrome.0.0j69i57j0l4.13398j0j7&sourceid=chrome&ie=UTF-8

  def ConvertSqFtToSqM(sqrFeet: Double ): Double  = {
    val sqrFeetToSqrMeter = 0.092903
    sqrFeet * sqrFeetToSqrMeter
  }

  def ConvertDolarToEuro(dolar : Double, exchangeValue : Double) : Double = {
    dolar * exchangeValue
  }

  //API para obtener el valor actual de la conversión de dolares a euros
  def getExchangeValue(): Double = {

    val url = "https://api.exchangeratesapi.io/latest?symbols=USD"
    val result = scala.io.Source.fromURL(url).mkString


    implicit val formats = DefaultFormats

    val obj = parse(result)
    (obj \ "rates" \ "USD").extract[Double]
  }


  def TransformarResultado(row : Inmueble,exchangedValue : Double)  : InmuebleTransformado = {
    val crecimientoAnual = 1.0f + Random.nextInt(1000) / 1000.0f + Random.nextInt(1000) / 1000.0f
    val sizeM  = (ConvertSqFtToSqM(row.Size) * crecimientoAnual).toInt
    val priceEU = (ConvertDolarToEuro(row.Price, exchangedValue) * crecimientoAnual).toInt
    val priceEuSQM = (priceEU / sizeM)


    //Generamos los ids de las locations
    var codeLocation = 0
    val location = row.Location
    val code = dictionary.get(location)
    if(code.isEmpty)
    {
      codeLocation = incrementalCode
      incrementalCode = incrementalCode + 1
      dictionary.put(location, codeLocation)
    }
    else
    {
      codeLocation = code.get
    }

    InmuebleTransformado(row.MLS, row.Location, codeLocation, priceEU, row.Bedrooms, row.Bathrooms,sizeM , priceEuSQM)
  }

  //driver program producto inmoviliario
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;


    import spark.implicits._

    //hacemos la ingesta de datos

    val esquema = new StructType().add("MLS", StringType, false).add("Location", StringType, true)
      .add("Price", FloatType, true).add("Bedrooms", IntegerType, true).add("Bathrooms", IntegerType, true)
      .add("Size", IntegerType, true).add("Price_SQ_FT", FloatType, true).add("Status", StringType, true)


    val inmueblesDS = spark.read
      .format("csv")
      .option("sep",",")
      .option("header", "true") //reading the headers
      .schema(esquema)
      .option("mode", "DROPMALFORMED")
      .csv(pathInitialCSV).as[Inmueble]

    inmueblesDS.printSchema

    val exchangedValue = 1 / getExchangeValue;

    //Aplicamos una transformacion para obtener los datos en euros y en metros cuadrados y lo guardaremos para el posterior análisis de machine learning.
    val inmuebleTransformadoFinal = inmueblesDS.map((inmueble) => TransformarResultado(inmueble, exchangedValue))

    inmuebleTransformadoFinal.createOrReplaceTempView("inmuebles")

    //obtenemos los datos de la localización, del size en metro y del precio en euros para el futuro análisis de machine learning.
    val inmueblesTransformML = spark.sql(
      """ SELECT LocationID, Bedrooms, Bathrooms, Size_M, Price_EU
        FROM inmuebles
        ORDER BY 1
      """)

    inmueblesTransformML.repartition(1).write.mode(SaveMode.Overwrite).option("header","false").csv(pathToSaveTransformML)

    val inmueblesCodesML = spark.sql(
      """ SELECT Location, LocationID
        FROM inmuebles
        GROUP BY 2,1
        ORDER BY 2
      """
    )

    inmueblesCodesML.repartition(1).write.mode(SaveMode.Overwrite).option("header","false").csv(pathToSaveTransformLocationsCodes)

    //agrupamos en 1 partición, y guardamos la información de la localización y la media del precio por metro cuadrado y lo ordenamos de mayor a menor.

    val inmueblesPrecioMedio = spark.sql(
      """ SELECT Location, AVG(Price_EU_SQ_M) as precioMedio
        FROM inmuebles
        GROUP BY Location
        ORDER BY 2 DESC

      """)

    inmueblesPrecioMedio.show

    //Guardamos el fichero en el directorio real-state

    inmueblesPrecioMedio.repartition(1).write.mode(SaveMode.Overwrite).json(pathToSaveAveragePrice)
  }
}
