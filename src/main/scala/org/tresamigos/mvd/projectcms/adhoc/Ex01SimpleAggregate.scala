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

import org.tresamigos.mvd.projectcms.core._
import org.tresamigos.mvd.projectcms.phase1._
import org.tresamigos.smv._
import org.apache.spark.sql.catalyst.expressions._

object Ex01SimpleAggregate extends SmvModule("Example 01: to demo simple use") {

  override def requiresDS() = Seq(
    CmsRaw
  )

  override def run(i: runParams) = {
    val srdd = i(CmsRaw)
    import srdd.sqlContext._

    srdd.groupBy('npi)(
      First('npi) as 'npi,
      Sum('line_srvc_cnt) as 'total_srvc
    )
  }
}

