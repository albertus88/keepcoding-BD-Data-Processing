package bdproc.practica_alberto_monitorizacion

import bdproc.common.EmailUtilities
import org.apache.spark.sql.{ForeachWriter, _}
import org.apache.spark.sql.functions._
import bdproc.common.Utilities._

object SparkMonitorizaciónPrecios {

  case class InmueblePrecioMedio(Location: String, precioMedio : Double)

  //método para chequear si se supera el límite, en el caso de ser así se concatena el mensaje al cuerpo para el posterior envio.
  def SendEmail( f : InmueblePrecioMedio, limitValue : Double, body : String ) : String = {

    var bodyConcat = body
    if(f.precioMedio >= limitValue)
    {
      println(s"El inmueble de ${f.Location} supera con el precio ${f.precioMedio} \n")
      bodyConcat = body.concat("El inmueble de "+ f.Location + " supera con el precio "  + f.precioMedio + "\n")
    }

    bodyConcat
  }

  def main(args: Array[String]) {

    //obtenemos el limitValue de los parameters de la configuración.
    val limitValue = args(0).toFloat
    //session spark
    val spark = SparkSession
      .builder
      .appName("Precios medio Monitor")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", pathToCheckPoints)
      .getOrCreate()

    setupLogging()

    spark.sql("set spark.sql.streaming.schemaInference=true")
    //set up monitorización directorio RealState
    val jsonLoaded = spark.readStream.json(pathToSaveAveragePrice)

    // Must import spark.implicits for conversion to DataSet to work!
    import spark.implicits._

    //agrupamos por Location en una ventana de una hora
    val windowedData = jsonLoaded.groupBy($"Location", $"precioMedio", window(current_timestamp(), "1 hour"))
      .count().orderBy($"precioMedio".desc)

    //filtrado de aquellos inmuebles que superen el límite de precio.
    val outlierPrize = windowedData.filter(col("precioMedio" ) >= limitValue).as[InmueblePrecioMedio]

    var body = ""

    //para poder ejecutar la función de email, primero hemos de juntar las particiones, y después ejecutar un foreach asíncrono para cuando
    //los datos estén presentes.
    outlierPrize.coalesce(1).writeStream.outputMode("complete").foreach(new ForeachWriter[InmueblePrecioMedio] {

      override def open(partitionId: Long, epochId: Long): Boolean =
      {
        true
      }

      override def process(value: InmueblePrecioMedio): Unit =
      {
        body = SendEmail(value, limitValue,body)
      }

      override def close(errorOrNull: Throwable): Unit =
      {
        if(body.length() > 0)
        {
          EmailUtilities.SendEmail(body)
          println("Report outliers prizes")
          println(body)
        }
      }
    }).start()



    //iniciar procedimient (query) y mostrar resultados por consola de modo 'complete'
    val query = windowedData.writeStream.outputMode("complete")
      .format("console").start


   //permitir finalización query
    query.awaitTermination()

    spark.stop()
  }
}
