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

import com.oceanbase.spark.OceanBaseMySQLConnectorITCase.expected
import com.oceanbase.spark.OceanBaseTestBase.assertEqualsInAnyOrder

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterAll, AfterEach, Assertions, BeforeAll, BeforeEach, Test}
import org.junit.jupiter.api.function.ThrowingSupplier

import java.util

class OBCatalogMySQLITCase extends OceanBaseMySQLTestBase {

  @BeforeEach
  def initEach(): Unit = {
    initialize("sql/mysql/products.sql")
  }

  @AfterEach
  def afterEach(): Unit = {
    dropTables("products", "products_no_pri_key", "products_full_pri_key")
  }

  val OB_CATALOG_CLASS = "com.oceanbase.spark.catalog.OceanBaseCatalog"

  @Test
  def testCatalogBase(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "products")
    queryAndVerifyTableData(session, "products", expected)

    insertTestData(session, "products_no_pri_key")
    queryAndVerifyTableData(session, "products_no_pri_key", expected)

    session.stop()
  }

  @Test
  def testCatalogOp(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    import scala.collection.JavaConverters._
    val dbList = session.sql("show databases").collect().map(_.toString()).toList.asJava
    val expectedDbList =
      Seq("[information_schema]", "[mysql]", "[oceanbase]", "[test]").toList.asJava
    assertEqualsInAnyOrder(expectedDbList, dbList)

    val tableList = session.sql("show tables").collect().map(_.toString()).toList.asJava
    val expectedTableList = Seq(
      "[test,products,false]",
      "[test,products_no_pri_key,false]",
      "[test,products_full_pri_key,false]").toList.asJava
    assertEqualsInAnyOrder(expectedTableList, tableList)

    // test create/drop namespace
    Assertions.assertDoesNotThrow(new ThrowingSupplier[Unit] {
      override def get(): Unit = {
        session.sql("create database spark")
        session.sql("use spark")
      }
    })
    val expectedCreateDbList =
      Seq("[information_schema]", "[mysql]", "[oceanbase]", "[test]", "[spark]").toList.asJava
    val dbList1 = session.sql("show databases").collect().map(_.toString()).toList.asJava
    assertEqualsInAnyOrder(expectedCreateDbList, dbList1)
    session.sql("drop database spark")
    val dbList2 = session.sql("show databases").collect().map(_.toString()).toList.asJava
    assertEqualsInAnyOrder(expectedDbList, dbList2)

    session.stop()
  }

  @Test
  def testCatalogJdbcInsertWithNoPriKeyTable(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "products_no_pri_key")
    queryAndVerifyTableData(session, "products_no_pri_key", expected)
    session.stop()
  }

  @Test
  def testCatalogJdbcInsertWithFullPriKeyTable(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "products_full_pri_key")

    queryAndVerifyTableData(session, "products_full_pri_key", expected)

    session.stop()
  }

  @Test
  def testCatalogDirectLoadWrite(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.defaultCatalog", "ob")
      .config("spark.sql.catalog.ob.direct-load.enabled", "true")
      .config("spark.sql.catalog.ob.direct-load.host", getHost)
      .config("spark.sql.catalog.ob.direct-load.rpc-port", getRpcPort)
      .config("spark.sql.catalog.ob.direct-load.username", getUsername.split("@").head)
      .getOrCreate()

    insertTestData(session, "products")
    queryAndVerifyTableData(session, "products", expected)

    insertTestData(session, "products_no_pri_key")
    session.sql("insert overwrite products select * from products_no_pri_key")
    queryAndVerifyTableData(session, "products", expected)
    session.stop()
  }

  @Test
  def testTableCreate(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .config("spark.sql.defaultCatalog", "ob")
      .getOrCreate()
    insertTestData(session, "products")
    // Test CTAS
    session.sql("create table test1 as select * from products")
    queryAndVerifyTableData(session, "test1", expected)

    // test bucket partition table:
    //   1. column comment test
    //   2. table comment test
    //   3. table options test
    session.sql("""
                  |CREATE TABLE test2(
                  |  user_id BIGINT COMMENT 'test_for_key',
                  |  name VARCHAR(255)
                  |)
                  |PARTITIONED BY (bucket(16, user_id))
                  |COMMENT 'test_for_table_create'
                  |TBLPROPERTIES('replica_num' = 2, COMPRESSION = 'zstd_1.0');
                  |""".stripMargin)
    val showCreateTable = getShowCreateTable(s"$getSchemaName.test2")
    Assertions.assertTrue(
      showCreateTable.contains("test_for_key")
        && showCreateTable.contains("test_for_table_create")
        && showCreateTable.contains("partition by key(`user_id`)")
        && showCreateTable.contains("COMPRESSION = 'zstd_1.0'")
        && showCreateTable.contains("REPLICA_NUM = 1"))
    dropTables("test1", "test2")
    session.stop()
  }

  @Test
  def testTruncateAndOverWriteTable(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "products")
    session.sql("truncate table products")
    val expect = new util.ArrayList[String]()
    queryAndVerifyTableData(session, "products", expect)

    insertTestData(session, "products_no_pri_key")
    session.sql("insert overwrite products select * from products_no_pri_key")
    queryAndVerifyTableData(session, "products", expected)

    session.stop()
  }

  @Test
  def testDeleteWhere(): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.ob", OB_CATALOG_CLASS)
      .config("spark.sql.catalog.ob.url", getJdbcUrl)
      .config("spark.sql.catalog.ob.username", getUsername)
      .config("spark.sql.catalog.ob.password", getPassword)
      .config("spark.sql.catalog.ob.schema-name", getSchemaName)
      .getOrCreate()

    session.sql("use ob;")
    insertTestData(session, "products")
    session.sql("delete from products where 1 = 0")
    queryAndVerifyTableData(session, "products", expected)

    session.sql("delete from products where id = 1")
    queryAndVerifyTableData(session, "products", expected)

    session.sql("delete from products where description is null")
    queryAndVerifyTableData(session, "products", expected)

    session.sql("delete from products where id in (101, 102, 103)")
    session.sql("delete from products where name = 'hammer'")

    session.sql("delete from products where name like 'rock%'")
    session.sql("delete from products where name like '%jack%' and id = 108 or weight = 5.3")
    session.sql("delete from products where id >= 109")

    val expect = new util.ArrayList[String]()
    queryAndVerifyTableData(session, "products", expect)

    session.stop()
  }

  private def queryAndVerifyTableData(
      session: SparkSession,
      tableName: String,
      expected: util.List[String]): Unit = {
    import scala.collection.JavaConverters._
    val actual = session
      .sql(s"select * from $tableName")
      .collect()
      .map(
        _.toString().drop(1).dropRight(1)
      )
      .toList
      .asJava
    assertEqualsInAnyOrder(expected, actual)
  }

  private def insertTestData(session: SparkSession, tableName: String): Unit = {
    session.sql(
      s"""
         |INSERT INTO $getSchemaName.$tableName VALUES
         |(101, 'scooter', 'Small 2-wheel scooter', 3.14),
         |(102, 'car battery', '12V car battery', 8.1),
         |(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.8),
         |(104, 'hammer', '12oz carpenter\\'s hammer', 0.75),
         |(105, 'hammer', '14oz carpenter\\'s hammer', 0.875),
         |(106, 'hammer', '16oz carpenter\\'s hammer', 1.0),
         |(107, 'rocks', 'box of assorted rocks', 5.3),
         |(108, 'jacket', 'water resistent black wind breaker', 0.1),
         |(109, 'spare tire', '24 inch spare tire', 22.2);
         |""".stripMargin)
  }
}

object OBCatalogMySQLITCase {
  @BeforeAll
  def setup(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.start()
  }

  @AfterAll
  def tearDown(): Unit = {
    OceanBaseMySQLTestBase.CONTAINER.stop()
  }
}
