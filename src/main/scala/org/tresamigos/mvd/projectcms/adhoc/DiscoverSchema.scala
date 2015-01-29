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

package org.tresamigos.mvd.projectcms.adhoc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.tresamigos.smv._

/** 
 * Discover Cms file Schema and create cms_raw.csv 
 */
object DiscoverSchema {
  val datafile = "data/cms/input/cms_raw_no_2nd_line.csv"

  def main(args: Array[String]) {
    implicit val ca = CsvAttributes.defaultTsvWithHeader

    val conf = new SparkConf().setAppName("DiscoverSchema")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val srdd = sqlContext.csvFileWithSchemaDiscovery(datafile, 1000000)
    val schema=Schema.fromSchemaRDD(srdd)
    schema.saveToFile(sc, "data/cms/input/cms_raw_no_2nd_line.schema")
  }
}

