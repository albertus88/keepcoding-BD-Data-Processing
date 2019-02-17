package bdproc.common

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher

object Utilities {

  val relatiPathToProjectWindows = "C:/Users/alber/OneDrive/Escritorio/"
  val relativePathToProjectVM = "mnt/hgfs/"
  val pathToFolder = "file:///"+ relativePathToProjectVM + "PracticaDataProcesing2/datasets/"
  val pathInitialCSV = pathToFolder + "RealEstate.csv"
  val pathToSaveTransformML = pathToFolder + "RealEstate_ML_2"
  val pathToSaveTransformLocationsCodes = pathToFolder + "RealEstate_ML_Codes"
  val pathToSaveAveragePrice = pathToFolder + "RealEstate_2"
  val pathToCheckPoints = pathToFolder + "checkpoints"
  val pathToRealStateFileML = pathToSaveTransformML + "/RealStateTransform.csv"

  case class Inmueble(MLS: String, Location: String, Price : Double, Bedrooms : Int, Bathrooms: Int, Size: Int, Price_SQ_FT: Double, Status: String)

  case class InmuebleTransformado(MLS: String, Location: String, LocationID : Int, Price_EU : Int, Bedrooms : Int, Bathrooms: Int, Size_M: Int, Price_EU_SQ_M: Int)

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

}
