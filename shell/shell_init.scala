import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions._
import org.tresamigos.smv._
import org.tresamigos.mvd.projectcms.core._
import org.tresamigos.mvd.projectcms.adhoc._
//import org.tresamigos.mvd.projectcms.phase1._
//import org.tresamigos.mvd.projectcms.phase2._

val app = new CmsApp("Cms App", Seq("-d", "None"), Option(sc))
val sqlContext = app.sqlContext
import sqlContext._

// create the init object "i" rather than create initialization at top level
// because shell would launch a separate command for each evalutaion which
// slows down startup considerably.
// keeping object name short to make the contents easy to access.
object i {
  //-------- some helpful functions
  def s(ds: SmvDataSet) = app.resolveRDD(ds)
  def open(fullPath: String) = {
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    sqlContext.csvFileWithSchema(fullPath)
  }
  def save(srdd: SchemaRDD, fullPath: String) = {
    srdd.saveAsCsvWithSchema(fullPath)(CsvAttributes.defaultCsvWithHeader)
  }
  
  val dataDir = sys.env.getOrElse("DATA_DIR", "/DATA_DIR_ENV_NOT_SET")

  //lazy val sample_data = open(dataDir + "/" + "input/sample.csv")
}
