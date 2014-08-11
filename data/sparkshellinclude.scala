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

import org.apache.log4j.{LogManager, Logger, Level}
import org.apache.spark.sql.SQLContext
import org.tresamigos.smv._
val sqlContext = new SQLContext(sc)
import sqlContext._
import org.apache.spark.sql.catalyst.expressions._

object SparkTestUtil {
  import scala.collection.JavaConversions.enumerationAsScalaIterator

  def setLoggingLevel(level: Level) {
    val rootLogger = LogManager.getRootLogger
    val loggers = rootLogger :: LogManager.getCurrentLoggers.map(_.asInstanceOf[Logger]).toList
    loggers.foreach { logger =>
      logger.setLevel(level)
    }
  }
}

SparkTestUtil.setLoggingLevel(Level.ERROR)
