package bdproc.practica_alberto

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import bdproc.common.Utilities._

import scala.util.Random

object SparkProductoInmobiliario
{


  //link : https://www.google.com/search?q=convertir+pies+cuadrados+a+metros+cuadrados&oq=convertir+pies+cuadrados+a+metros+cuadr&aqs=chrome.0.0j69i57j0l4.13398j0j7&sourceid=chrome&ie=UTF-8

  def ConvertSqFtToSqM(sqrFeet: Float )  = {
    val sqrFeetToSqrMeter = 0.092903f
    sqrFeet * sqrFeetToSqrMeter
  }

  //link : https://www.google.com/search?q=cambio+dolar+euro&oq=cambio+dolar+euro&aqs=chrome..69i57j0l5.3240j1j7&sourceid=chrome&ie=UTF-8
  def ConvertDolarToEuro(dolar : Float) = {
    val dolarToEuro = 0.88f
    dolar * dolarToEuro
  }

  def TransformarResultado(row : Inmueble)  : InmuebleTransformado = {
    val crecimientoAnual = 1.0f + Random.nextInt(1000) / 1000.0f + Random.nextInt(1000) / 1000.0f
    val sizeM  = ConvertSqFtToSqM(row.Size) * crecimientoAnual
    val priceEU = ConvertDolarToEuro(row.Price) * crecimientoAnual
    val priceEuSQM = priceEU / sizeM
    InmuebleTransformado(row.MLS, row.Location, priceEU, row.Bedrooms, row.Bathrooms,sizeM , priceEuSQM)
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

    //Aplicamos una transformacion para obtener los datos en euros y en metros cuadrados y lo guardaremos para el posterior análisis de machine learning.
    val inmuebleTransformadoFinal = inmueblesDS.map(TransformarResultado)

    //sentencia directa

    inmuebleTransformadoFinal.createOrReplaceTempView("inmuebles")

    val inmueblesTransformML = spark.sql(
      """ SELECT Location, Size_M, Price_EU
        FROM inmuebles
      """)

    inmueblesTransformML.persist().coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(pathToSaveTransformML)

    val inmueblesPrecioMedio = spark.sql(
      """ SELECT Location, AVG(Price_EU_SQ_M) as precioMedio
        FROM inmuebles
        GROUP BY Location
        ORDER BY 2 DESC

      """)

    //Guardamos el fichero en el directorio real-state

    inmueblesPrecioMedio.show

    inmueblesPrecioMedio.persist().coalesce(1).write.mode(SaveMode.Overwrite).json(pathToSaveAveragePrice)
  }
}
