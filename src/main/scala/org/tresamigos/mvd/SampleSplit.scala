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
import org.tresamigos.smv._

/* Demo: load raw data as RDD[String] and do
   - remove header
   - hash split
   - hash sample
   - save as gzip files
*/
object SampleSplit {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  val datadir = "data/CMS_Data/"

  val raw = datadir + "raw"
  val schema = datadir + "schema"
  val all = datadir + "all"
  val sampled = datadir + "sampled"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SampleSplit")
    val sc = new SparkContext(conf)

    val raw_rdd = sc.textFile(raw, 1)
    val p_rdd = raw_rdd.pipe("sed -e 1,2d").csvAddKey()(delimiter='\t').hashPartition(8)
    val sampled_rdd = p_rdd.csvAddKey()(delimiter='\t').hashSample(0.05)

    p_rdd.saveAsGZFile(all)
    sampled_rdd.saveAsGZFile(sampled)

  }
}
