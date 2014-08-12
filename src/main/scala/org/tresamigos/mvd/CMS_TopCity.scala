/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.mvd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.tresamigos.smv._

/* Demo: 
Load schemaRDD and run sumary on a group of doubles
*/
object CMS_TopCity {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  val indata = "data/CMS_Data/all"
  val schema = "data/CMS_Data/schema"
  val outreport = "data/CMS_Data/report/topCity"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CMS_TopCity")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val filein = sqlContext.csvFileWithSchema(indata, schema, delimiter = '\t')

    val top50 = Set("NEW YORK", "HOUSTON", "CHICAGO", "SPRINGFIELD", 
      "PHILADELPHIA", "BROOKLYN", "DALLAS", "BALTIMORE", "COLUMBUS", 
      "LOS ANGELES", "SAN ANTONIO", "BOSTON", "SAINT LOUIS", "ROCHESTER", 
      "JACKSONVILLE", "CLEVELAND", "ATLANTA", "INDIANAPOLIS", "PHOENIX", 
      "BIRMINGHAM", "LOUISVILLE", "GREENVILLE", "JACKSON", "COLUMBIA", 
      "LAS VEGAS", "AUSTIN", "NASHVILLE", "RICHMOND", "CHARLOTTE", 
      "PORTLAND", "SEATTLE", "PITTSBURGH", "CINCINNATI", "MIAMI", 
      "WASHINGTON", "GAINESVILLE", "TAMPA", "SAN DIEGO", "MADISON", 
      "TUCSON", "KANSAS CITY", "KNOXVILLE", "OKLAHOMA CITY", "WILMINGTON", 
      "OMAHA", "ORLANDO", "CHARLESTON", "MILWAUKEE", "LITTLE ROCK", 
      "TULSA")

    val get50: String => String = {c =>
      if (top50.contains(c)) c else "Other"
    }

    val srdd = filein.select(
        'npi,
        'nppes_credentials,
        'nppes_provider_gender,
        'nppes_entity_code,
        'nppes_provider_city,
        ScalaUdf(get50, StringType, Seq('nppes_provider_city)) as 'city,
        LEFT('nppes_provider_zip,5) as 'zip5,
        'nppes_provider_state,
        'nppes_provider_country,
        'provider_type,
        'medicare_participation_indicator,
        'place_of_service,
        'hcpcs_code,
        'hcpcs_description,
        'line_srvc_cnt,
        'bene_unique_cnt,
        'bene_day_srvc_cnt,
        'average_Medicare_allowed_amt,
        'stdev_Medicare_allowed_amt,
        'average_submitted_chrg_amt,
        'stdev_submitted_chrg_amt,
        'average_Medicare_payment_amt,
        'stdev_Medicare_payment_amt       
    )

    val edd=srdd.groupEdd('city).addBaseTasks()

    edd.addAmountHistogramTasks(
      'average_Medicare_allowed_amt,
      'average_submitted_chrg_amt,
      'average_Medicare_payment_amt,
      'line_srvc_cnt,
      'bene_unique_cnt,
      'bene_day_srvc_cnt
    )

    edd.addHistogramTasks(
        'nppes_credentials,
        'nppes_provider_gender,
        'nppes_entity_code,
        'provider_type,
        'place_of_service,
        'hcpcs_code,
        'hcpcs_description
    )(byFreq = true)

    edd.createReport.saveAsGZFile(outreport)
  }
}
