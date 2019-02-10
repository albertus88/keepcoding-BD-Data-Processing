package bdproc.practica_alberto_monitorizacion

import bdproc.common.EmailUtilities
import org.apache.spark.sql.{ForeachWriter, _}
import org.apache.spark.sql.functions._
import bdproc.common.EmailUtilities._
import bdproc.common.Utilities._

object SparkMonitorizaciónPrecios {

  case class InmueblePrecioMedio(Location: String, precioMedio : Double)

  private val _enabledEmail = false

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

    val limitValue = args(0).toFloat
    //session spark
    val spark = SparkSession
      .builder
      .appName("Precios medio Monitor")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/Users/alber/OneDrive/Escritorio/KeepCodingGitlab/MóduloDataProcessing/practica/datasets/checkpoints")
      .getOrCreate()

    setupLogging()

    spark.sql("set spark.sql.streaming.schemaInference=true")
    //set up monitorización directorio RealState
    val jsonLoaded = spark.readStream.json("file:///C:/Users/alber/OneDrive/Escritorio/KeepCodingGitlab/MóduloDataProcessing/practica/datasets/RealEstate_2")

    // Must import spark.implicits for conversion to DataSet to work!
    import spark.implicits._

    // Group by status code, with a one-hour window.
    //todo: agrupamos por Location en una ventana de una hora
    val windowedData = jsonLoaded.groupBy($"Location", $"precioMedio", window(current_timestamp(), "1 hour"))
      .count().orderBy($"precioMedio".desc)

    val outlierPrize = windowedData.filter(col("precioMedio" ) >= limitValue).as[InmueblePrecioMedio]


    var body = ""

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
          if(_enabledEmail) {
            EmailUtilities.SendEmail(body)
          }

          println("Report outliers prizes")
          println(body)
        }
      }
    }).start()



    //todo: iniciar procedimient (query) y mostrar resultados por consola de modo 'complete'
    val query = windowedData.writeStream.outputMode("complete")
      .format("console").start


    //todo: permitir finalización query
    query.awaitTermination()

    spark.stop()
  }
}
