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

/* Demo: Load CSV file with schema to schemaRDD and 
run simpe spark.sql native expression */
object Demo001 {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  val indata = "data/CMS_Data/tiny"
  val schema = "data/CMS_Data/schema"
  val outdata = "target/classes/data/out/Total_srvc"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Demo001")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    import org.apache.spark.sql.catalyst.expressions._
    import org.tresamigos.smv.CsvAttributes.defaultTsv

    val srdd = sqlContext.csvFileWithSchema(indata, schema)

    val result=srdd.groupBy('npi)(
      First('npi) as 'npi, 
      Sum('line_srvc_cnt) as 'total_srvc
    )

    result.saveAsCsvWithSchema(outdata)

  }
}
