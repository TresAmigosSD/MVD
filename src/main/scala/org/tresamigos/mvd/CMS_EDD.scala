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
import org.tresamigos.smv._

/* Demo: 
Load schemaRDD and run sumary on a group of doubles
*/
object CMS_EDD {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  val indata = "data/CMS_Data/all"
  val schema = "data/CMS_Data/schema"
  val outreport = "data/CMS_Data/report/edd"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CMS_EDD")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val filein = sqlContext.csvFileWithSchema(indata, schema, delimiter = '\t')

    val srdd = filein.select(
        'npi,
        'nppes_credentials,
        'nppes_provider_gender,
        'nppes_entity_code,
        'nppes_provider_city,
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

    val edd=srdd.edd.addBaseTasks()

    edd.addAmountHistogramTasks(
      'average_Medicare_allowed_amt,
      'stdev_Medicare_allowed_amt,
      'average_submitted_chrg_amt,
      'stdev_submitted_chrg_amt,
      'average_Medicare_payment_amt,
      'stdev_Medicare_payment_amt
    )

    edd.addHistogramTasks(
        'nppes_credentials,
        'nppes_provider_gender,
        'nppes_entity_code,
        'nppes_provider_city,
        'zip5,
        'nppes_provider_state,
        'nppes_provider_country,
        'provider_type,
        'medicare_participation_indicator,
        'place_of_service,
        'hcpcs_code,
        'hcpcs_description,
        'line_srvc_cnt,
        'bene_unique_cnt,
        'bene_day_srvc_cnt
    )(byFreq = true, binSize = 20)

    edd.createReport.saveAsGZFile(outreport)
  }
}
