package bdproc.common

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher

object Utilities {

  val pathInitialCSV = "file:///mnt/hgfs/PracticaDataProcesing/datasets/RealEstate.csv"
  val pathToSaveTransformML = "file:///mnt/hgfs/PracticaDataProcesing/datasets/RealEstate_ML_2"
  val pathToSaveAveragePrice = "file:///mnt/hgfs/PracticaDataProcesing/datasets/RealEstate_2"

  case class Inmueble(MLS: String, Location: String, Price : Float, Bedrooms : Int, Bathrooms: Int, Size: Int, Price_SQ_FT: Float, Status: String)

  case class InmuebleTransformado(MLS: String, Location: String, Price_EU : Float, Bedrooms : Int, Bathrooms: Int, Size_M: Float, Price_EU_SQ_M: Float)

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

}
