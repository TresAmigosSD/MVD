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

package org.tresamigos.mvd.projectcms.core

import org.apache.spark.SparkContext
import org.tresamigos.smv._

/**
 * Driver for all CMS applications.
 * The app name is taken as a parameter to allow individual modules to override the app name
 * in their standalone run mode.
 */
class CmsApp(_appName: String,
                  _args: Seq[String],
                  _sc: Option[SparkContext] = None
                  )
      extends SmvApp(_appName, _args, _sc) {

  val num_partitions = sys.env.getOrElse("CMS_PARTITIONS", "32")
  sqlContext.setConf("spark.sql.shuffle.partitions", num_partitions)

  override def getModulePackages() = Seq(
    "org.tresamigos.mvd.projectcms.phase1",
    "org.tresamigos.mvd.projectcms.phase2"
  )
}

object CmsApp {
  val ca = CsvAttributes.defaultCsvWithHeader
  val caTsv = CsvAttributes.defaultTsvWithHeader

  // The file path is relative to $DATA_DIR as a environment variable
  val cms_raw_no_2nd_line =  SmvFile("cms_raw_no_2nd_line", "input/cms_raw_no_2nd_line.csv", caTsv)

  def main(args: Array[String]) {
    new CmsApp("CmsApp", args).run()
  }
}

