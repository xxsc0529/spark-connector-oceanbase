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

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

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
   * Compares executeBatch round-trips with rewriteBatchedStatements=true (the connector's default)
   * vs false, using the exact array text the writer emits.
   */
  @Test
  def probeRewriteBatch(): Unit = {
    Seq("false", "true").foreach {
      rw =>
        val conn = DriverManager.getConnection(
          getJdbcUrl + "&rewriteBatchedStatements=" + rw,
          getUsername,
          getPassword)
        try {
          val st = conn.createStatement()
          st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_rw")
          st.execute(
            s"CREATE TABLE $getSchemaName.t_arr_rw (id INT PRIMARY KEY, arr ARRAY(VARCHAR(255)))")
          val ps =
            conn.prepareStatement(s"INSERT INTO $getSchemaName.t_arr_rw (id, arr) VALUES (?, ?)")
          ps.setInt(1, 1)
          ps.setString(2, "[null, \"换行\\\\n值\"]")
          ps.addBatch()
          ps.setInt(1, 2)
          ps.setString(2, "[\"a\\\"b\", \"x,y\"]")
          ps.addBatch()
          ps.executeBatch()
          val rs = st.executeQuery(s"SELECT id, arr FROM $getSchemaName.t_arr_rw ORDER BY id")
          while (rs.next()) {
            System.out.println(
              s"PROBE_RW rewrite=$rw id=" + rs.getInt(1) + " value=" + visualize(rs.getString(2)))
          }
          st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_rw")
          st.close()
        } finally {
          conn.close()
        }
    }
  }

  /** Compares executeBatch with the exact upsert SQL shape the JDBC writer generates. */
  @Test
  def probeUpsertBatch(): Unit = {
    Seq("false", "true").foreach {
      rw =>
        val conn = DriverManager.getConnection(
          getJdbcUrl + "&rewriteBatchedStatements=" + rw,
          getUsername,
          getPassword)
        try {
          val st = conn.createStatement()
          st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_up")
          st.execute(
            s"CREATE TABLE $getSchemaName.t_arr_up (id INT PRIMARY KEY, arr ARRAY(VARCHAR(255)))")
          val upsertSql =
            s"INSERT  INTO $getSchemaName.t_arr_up " +
              "(`id`, `arr`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `arr` = VALUES(`arr`)"
          val ps = conn.prepareStatement(upsertSql)
          ps.setInt(1, 1)
          ps.setString(2, "[null, \"换行\\\\n值\"]")
          ps.addBatch()
          ps.setInt(1, 2)
          ps.setString(2, "[\"a\\\"b\", \"x,y\"]")
          ps.addBatch()
          ps.executeBatch()
          val rs = st.executeQuery(s"SELECT id, arr FROM $getSchemaName.t_arr_up ORDER BY id")
          while (rs.next()) {
            System.out.println(
              s"PROBE_UP rewrite=$rw id=" + rs.getInt(1) + " value=" + visualize(rs.getString(2)))
          }
          st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_up")
          st.close()
        } finally {
          conn.close()
        }
    }
  }

  /**
   * Runs the real catalog INSERT through a logging JDBC driver wrapper, printing the exact SQL text
   * and the exact setString payloads the writer produces.
   */
  @Test
  def probeRealPathWithLogging(): Unit = {
    DriverManager.registerDriver(new LoggingMysqlDriver())
    val logUrl = getJdbcUrl.replace("jdbc:mysql://", "jdbc:logmysql://")
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", "com.oceanbase.spark.catalog.OceanBaseCatalog")
      .config("spark.sql.catalog.ob.url", logUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()
    val conn = getJdbcConnection()
    val st = conn.createStatement()
    try {
      st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_real")
      st.execute(
        s"CREATE TABLE $getSchemaName.t_arr_real (id INTEGER NOT NULL PRIMARY KEY, arr ARRAY(VARCHAR(255)))")
      session.sql("use ob;")
      session.sql(s"""
                     |INSERT INTO $getSchemaName.t_arr_real VALUES
                     |(3, array(null, '换行\\n值')),
                     |(2, array('a\"b', 'x,y'))
                     |""".stripMargin)
      val rs = st.executeQuery(s"SELECT id, arr FROM $getSchemaName.t_arr_real ORDER BY id")
      while (rs.next()) {
        System.out.println(
          "PROBE_REAL_RAW id=" + rs.getInt(1) + " value=" + visualize(rs.getString(2)))
      }
    } finally {
      session.stop()
      st.execute(s"DROP TABLE IF EXISTS $getSchemaName.t_arr_real")
      st.close()
      conn.close()
    }
  }

  /** Prints how Spark parses the SQL string literals used by the real test. */
  @Test
  def probeSparkLiterals(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      System.out.println(
        "PROBE_LITERAL escapedStringLiterals=" +
          (try session.conf.get("spark.sql.parser.escapedStringLiterals")
          catch { case _: Exception => "unset" }))
      // same SQL text as the real test receives (Scala triple-quoted source keeps two backslashes)
      val v1 = session.sql("SELECT '换行\\\\n值'").collect().head.getString(0)
      System.out.println("PROBE_LITERAL 2BS: " + visualize(v1))
      val v2 = session.sql("SELECT 'a\\\\b'").collect().head.getString(0)
      System.out.println("PROBE_LITERAL 2BS+b: " + visualize(v2))
      val v3 = session.sql("SELECT '换行\\\\\\\\n值'").collect().head.getString(0)
      System.out.println("PROBE_LITERAL 4BS: " + visualize(v3))
      val v4 = session.sql("SELECT 'a\"b'").collect().head.getString(0)
      System.out.println("PROBE_LITERAL BS+Q: " + visualize(v4))
    } finally {
      session.stop()
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

/**
 * JDBC driver delegating jdbc:logmysql: URLs to the MySQL driver while logging SQL text and string
 * parameters.
 */
class LoggingMysqlDriver extends java.sql.Driver {
  private val delegate = new com.mysql.cj.jdbc.Driver()

  override def acceptsURL(url: String): Boolean = url != null && url.startsWith("jdbc:logmysql:")

  override def connect(url: String, info: Properties): Connection = {
    if (!acceptsURL(url)) return null
    val real = delegate.connect(url.replace("jdbc:logmysql:", "jdbc:mysql:"), info)
    if (real == null) null else wrapConnection(real)
  }

  override def getPropertyInfo(url: String, info: Properties) =
    delegate.getPropertyInfo(url, info)

  override def getMajorVersion: Int = delegate.getMajorVersion

  override def getMinorVersion: Int = delegate.getMinorVersion

  override def jdbcCompliant(): Boolean = delegate.jdbcCompliant()

  override def getParentLogger = delegate.getParentLogger

  private def visualizeText(s: String): String = {
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

  private def call(target: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
    if (args == null) method.invoke(target) else method.invoke(target, args: _*)
  }

  private def wrapStatement(ps: PreparedStatement): PreparedStatement = {
    Proxy
      .newProxyInstance(
        classOf[PreparedStatement].getClassLoader,
        Array(classOf[PreparedStatement]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            if (method.getName == "setString" && args != null && args.length == 2) {
              System.out.println(
                "PROBE_SET param" + args(0) + "=" + visualizeText(String.valueOf(args(1))))
            } else if (
              (method.getName == "executeBatch" || method.getName == "execute") &&
              (args == null || args.isEmpty)
            ) {
              System.out.println("PROBE_EXEC " + method.getName)
            }
            call(ps, method, args)
          }
        }
      )
      .asInstanceOf[PreparedStatement]
  }

  private def wrapConnection(conn: Connection): Connection = {
    Proxy
      .newProxyInstance(
        classOf[Connection].getClassLoader,
        Array(classOf[Connection]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            if (
              method.getName == "prepareStatement" && args != null && args.nonEmpty &&
              args(0).isInstanceOf[String]
            ) {
              System.out.println(
                "PROBE_SQL " + args(0).asInstanceOf[String].replaceAll("\\s+", " "))
              return wrapStatement(call(conn, method, args).asInstanceOf[PreparedStatement])
            }
            call(conn, method, args)
          }
        }
      )
      .asInstanceOf[Connection]
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
