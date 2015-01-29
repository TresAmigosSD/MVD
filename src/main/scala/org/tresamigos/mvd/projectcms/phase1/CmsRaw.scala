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

package org.tresamigos.mvd.projectcms.phase1

import org.tresamigos.mvd.projectcms.core._
import org.tresamigos.smv._

object CmsRaw extends SmvModule("Load cms raw data") {

  override def requiresDS() = Seq(
    CmsApp.cms_raw_no_2nd_line
  )

  override def run(i: runParams) = {
    val srdd = i(CmsApp.cms_raw_no_2nd_line)
    srdd
  }
}

