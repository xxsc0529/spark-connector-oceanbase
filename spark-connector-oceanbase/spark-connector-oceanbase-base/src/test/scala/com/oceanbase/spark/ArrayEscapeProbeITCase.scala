/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oceanbase.spark

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

/** TEMPORARY DIAGNOSTIC - DO NOT MERGE. Prints raw ARRAY text round-trip behavior. */
class ArrayEscapeProbeITCase extends OceanBaseMySQLTestBase {

  private def visualize(s: String): String = {
    if (s == null) return "null"
    s.flatMap {
      case '\\' => "<BS>"
      case '\n' => "<NL>"
      case '\r' => "<CR>"
      case '\t' => "<TAB>"
      case '"' => "<Q>"
      case c => c.toString
    }
  }

  @Test
  def probeArrayEscapes(): Unit = {
    val conn = getJdbcConnection()
    try {
      val st = conn.createStatement()
      val rs0 = st.executeQuery("SELECT VERSION()")
      rs0.next()
      System.out.println("PROBE_VERSION " + rs0.getString(1))

      st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_probe")
      st.execute(
        s"CREATE TABLE $getSchemaName.t_arr_probe (id INT PRIMARY KEY, arr ARRAY(VARCHAR(255)))")
      val ps =
        conn.prepareStatement(s"INSERT INTO $getSchemaName.t_arr_probe (id, arr) VALUES (?, ?)")

      // id -> input array-text (as delivered to the server, i.e. what the connector
      // would pass to PreparedStatement.setString)
      val inputs = Seq(
        // current writer output for Spark value 换行\n值 (backslash + n): two backslashes
        (1, "[null, \"换行\\\\n值\"]"),
        // one backslash + n
        (2, "[null, \"换行\\n值\"]"),
        // four backslashes + n (double-escaped variant)
        (3, "[null, \"换行\\\\\\\\n值\"]"),
        // current writer output for Spark value a"b
        (4, "[\"a\\\"b\", \"x,y\"]"),
        // two backslashes + b
        (5, "[\"a\\\\b\"]"),
        // one backslash + t
        (6, "[\"a\\tb\"]"),
        // verbatim real newline inside the element
        (7, "[\"换行" + '\n' + "值\"]"),
        // verbatim real tab inside the element
        (8, "[\"a" + '\t' + "b\"]")
      )
      inputs.foreach {
        case (id, text) =>
          try {
            ps.setInt(1, id)
            ps.setString(2, text)
            ps.execute()
            System.out.println(s"PROBE_SEND_OK id=$id " + visualize(text))
          } catch {
            case e: Exception =>
              System.out.println(
                s"PROBE_SEND_FAIL id=$id " + visualize(text) + " err=" +
                  e.getMessage.replaceAll("\\s+", " ").take(100))
          }
      }

      val rs = st.executeQuery(s"SELECT id, arr FROM $getSchemaName.t_arr_probe ORDER BY id")
      val md = rs.getMetaData
      while (rs.next()) {
        System.out.println(
          s"PROBE_RECV id=${rs.getInt(1)} sqlType=${md.getColumnType(2)} typeName=${md.getColumnTypeName(2)} value=" +
            visualize(rs.getString(2)))
      }
      st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_probe")
      st.close()
    } finally {
      conn.close()
    }
  }

  /**
   * Replicates the real catalog test flow: Spark SQL INSERT, then compare raw JDBC read vs Spark
   * reader output.
   */
  @Test
  def probeViaSparkCatalog(): Unit = {
    val conn = getJdbcConnection()
    val st = conn.createStatement()
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", "com.oceanbase.spark.catalog.OceanBaseCatalog")
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()
    try {
      st.execute(s"DROP TABLE IF EXISTS $getSchemaName.products_string_arrays")
      st.execute(
        s"CREATE TABLE $getSchemaName.products_string_arrays (id INTEGER NOT NULL PRIMARY KEY, interests ARRAY(VARCHAR(255)))")
      session.sql("use ob;")
      session.sql(s"""
                     |INSERT INTO $getSchemaName.products_string_arrays VALUES
                     |(3, array(null, '换行\\n值')),
                     |(2, array('a\"b', 'x,y'))
                     |""".stripMargin)
      val rs = st.executeQuery(
        s"SELECT id, interests FROM $getSchemaName.products_string_arrays ORDER BY id")
      while (rs.next()) {
        System.out.println("PROBE_RAW id=" + rs.getInt(1) + " value=" + visualize(rs.getString(2)))
      }
      session
        .sql(s"SELECT * FROM $getSchemaName.products_string_arrays ORDER BY id")
        .collect()
        .foreach(r => System.out.println("PROBE_SPARK " + visualize(r.toString())))
    } finally {
      session.stop()
      st.execute(s"DROP TABLE IF EXISTS $getSchemaName.products_string_arrays")
      st.close()
      conn.close()
    }
  }
}

object ArrayEscapeProbeITCase extends OceanBaseMySQLTestBase {
  @BeforeAll
  def setup(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.start()
  }

  @AfterAll
  def tearDown(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.stop()
  }
}
