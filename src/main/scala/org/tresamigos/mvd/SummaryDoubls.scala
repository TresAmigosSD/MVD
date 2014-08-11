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

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

/* Demo: 
Load schemaRDD and run sumary on a group of doubles
*/
object SummaryDoubles {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  val indata = "data/CMS_Data/all"
  val schema = "data/CMS_Data/schema"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SummaryDoubles")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val srdd = sqlContext.csvFileWithSchema(indata, schema, delimiter = '\t')

    val varlist = Array(
      'line_srvc_cnt, 
      'bene_unique_cnt, 
      'bene_day_srvc_cnt, 
      'average_Medicare_allowed_amt, 
      'average_submitted_chrg_amt, 
      'average_Medicare_payment_amt
    )

    val sr_doubles = srdd.select(
      varlist.map(symbolToUnresolvedAttribute): _*
    )

    println(varlist.mkString(","))

    val sr_vector = sr_doubles.map(
      _.toArray.map(l=>l.asInstanceOf[Double])
    ).map(Vectors.dense(_))

    val mat = new RowMatrix(sr_vector)

    val sum = mat.computeColumnSummaryStatistics()

    println("count:       %s".format(sum.count))
    println("mean:        %s".format(sum.mean))
    println("max:         %s".format(sum.max))
    println("min:         %s".format(sum.min))
    println("variance:    %s".format(sum.variance))
    println("numNonzeros: %s".format(sum.numNonzeros))

  }
}
