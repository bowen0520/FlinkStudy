package com.zjhc.rtapp.utils

import java.util
import java.util.{ArrayList, List}

import com.zjhc.rtapp.sink.KuduOutputFormat
import com.zjhc.rtapp.utils.exceptions.{KuduClientException, KuduTableException}
import org.apache.flink.types.Row
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{CreateTableOptions, Delete, Insert, KuduClient, KuduException, KuduScanner, KuduSession, KuduTable, OperationResponse, PartialRow, RowResult, RowResultIterator, Update, Upsert}
import org.apache.log4j.Logger

class Util(host: String){
  //Kudu variables（变量）
  private var client:KuduClient = null
  private var session:KuduSession = null
  //初始化client和session
  this.client = new KuduClient.KuduClientBuilder(host).build
  if (client == null) {//无法通过host访问不到kudu客户端，抛出kudu客户端异常
    throw new KuduClientException("ERROR: param \"host\" not valid, can't establish connection")
  }
  this.session = this.client.newSession

  // log4j：获取logger日志对象
  private val logger = Logger.getLogger(this.getClass)

  /**
   * 通过表名和操作获取表对象
   *
   * @param tableName 要查找的表
   * @param tableMode 对要查找的表做的操作(CREATE, APPEND, OVERRIDE)
   * @return 返回查找到的表
   * @throws KuduTableException
   */
  def useTable(tableName: String, tableMode: Int): KuduTable = {
    var table:KuduTable = null
    if (tableMode == KuduOutputFormat.CREATE) {
      //操作为创建表，使用重载方法useTable(String tableName, String [] fieldsNames, Row row)
      logger.error("Bad call method, use useTable(String tableName, String [] fieldsNames, Row row) instead")
      table = null
    } else if (tableMode == KuduOutputFormat.APPEND) {
      //往表内追加内容
      logger.info("Modo APPEND")
      try {
        //判断表是否存在，存在直接获取对象，否则抛异常
        if (client.tableExists(tableName)) {
          table = client.openTable(tableName)
        } else {
          logger.error("ERROR: The table doesn't exist")
          throw new KuduTableException("ERROR: The table doesn't exist, so can't do APPEND operation")
        }
      }catch {
        case e: Exception =>
          throw new KuduTableException("ERROR: param \"host\" not valid, can't establish connection")
      }
    } else if (tableMode == KuduOutputFormat.OVERRIDE) {
      //覆盖表内容
      logger.info("Modo OVERRIDE")
      try {
        //如果表存在，清空表内容，否则抛异常
        if (client.tableExists(tableName)) {
          logger.info("SUCCESS: There is the table with the name \"" + tableName + "\". Emptying the table")
          clearTable(tableName)
          table = client.openTable(tableName)
        } else {
          logger.error("ERROR: The table doesn't exist")
          throw new KuduTableException("ERROR: The table doesn't exist, so can't do OVERRIDE operation")
        }
      } catch {
        case e: Exception =>
          throw new KuduTableException("ERROR: param \"host\" not valid, can't establish connection")
      }
    } else {
      throw new KuduTableException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableMode\" parameter.")
    }
    table
  }

  /**
   * 如果存在直接获取表，否则创建表返回
   *
   * @param tableName 需要获取的表的表名
   * @param fieldsNames 表不存在时，创建所需的字段名称
   * @param row 表字段的数据类型
   * @return Instance of the table indicated
   * @throws IllegalArgumentException 参数异常
   */
  def useTable(tableName: String, fieldsNames: Array[String], row: Row): KuduTable = {
    var table:KuduTable = null
    if (client.tableExists(tableName)) {
      logger.info("The table exists")
      table = client.openTable(tableName)
    } else if (tableName == null || tableName == "") {
      throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"tableName\" parameter.")
    }else if (fieldsNames == null || fieldsNames(0).isEmpty) {
      throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Missing \"fields\" parameter.")
    } else if (row == null) {
      throw new IllegalArgumentException("ERROR: Incorrect parameters, please check the constructor method. Incorrect \"row\" parameter.")
    } else {
      logger.info("The table doesn't exist")
      table = createTable(tableName, fieldsNames, row)
    }
    table
  }

  /**
   * 创建并返回该表
   *
   * @param tableName 表名
   * @param fieldsNames 各字段名
   * @param row 内部存储了各字段数据类型
   * @return
   */
  def createTable(tableName: String, fieldsNames: Array[String], row: Row): KuduTable = {
    if (client.tableExists(tableName)) return client.openTable(tableName)
    val columns = new util.ArrayList[ColumnSchema]
    val rangeKeys = new util.ArrayList[String] // Primary key
    rangeKeys.add(fieldsNames(0))
    logger.info("Creating the table \"" + tableName + "\"...")
    for (i <- 0 until fieldsNames.length) {
      var col:ColumnSchema = null
      val colName = fieldsNames(i)
      val colType = getRowsPositionType(i, row)
      if (colName == fieldsNames(0)) {
        col = new ColumnSchema.ColumnSchemaBuilder(colName, colType).key(true).build
        columns.add(0, col) //主键需要唯一
      } else {
        col = new ColumnSchema.ColumnSchemaBuilder(colName, colType).build
        columns.add(col)
      }
    }
    val schema = new Schema(columns)
    if (!client.tableExists(tableName)) {
      client.createTable(tableName, schema, new CreateTableOptions().setNumReplicas(1).setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, 4))
    }
    logger.info("SUCCESS: The table has been created successfully");
    client.openTable(tableName)
  }

  /**
   * 删除表
   *
   * @param tableName 表名
   */
  def deleteTable(tableName: String): Unit = {
    logger.info("Deleting the table \"" + tableName + "\"...")
    try {
      if (client.tableExists(tableName)) {
        client.deleteTable(tableName)
        logger.info("SUCCESS: Table deleted successfully")
      }
    } catch {
      case e: KuduException =>
        logger.error("The table \"" + tableName + "\" doesn't exist, so can't be deleted.", e)
    }
  }

  /**
   * 返回pos索引所对应字段的数据类型
   *
   * @param pos 字段所处的索引
   * @param row 存储了所有字段的数据类型
   * @return
   */
  def getRowsPositionType(pos: Int, row: Row): Type = TableUtil.getType(row.getField(pos).getClass)

  /**
   * 返回表的所有行的列表集
   *
   * @param tableName
   * @return
   */
  def readTable(tableName: String): util.List[Row] = {
    val table = client.openTable(tableName)
    val scanner = client.newScannerBuilder(table).build
    var posRow = 0
    val columnsNames = getNamesOfColumns(table)
    val rowsList = new util.ArrayList[Row]
    while (scanner.hasMoreRows) {
      import scala.collection.JavaConversions._
      for (row:RowResult  <- scanner.nextRows.iterator()) {
        val rowToInsert = new Row(columnsNames.length)
        for (col <- columnsNames) {
          rowToInsert.setField(posRow, TableUtil.valueFromRow(row, col))
          posRow += 1
        }
        rowsList.add(rowToInsert)
        posRow = 0
      }
    }
    rowsList
  }


  /**
   * 打印表的所有内容
   *
   * @param tableName
   */
  def readTablePrint(tableName: String): Unit = {
    val table = client.openTable(tableName)
    val scanner = client.newScannerBuilder(table).build
    var cont = 0 //表中数据行数
    try {
      while (scanner.hasMoreRows) {
        val results = scanner.nextRows
        while (results.hasNext) {
          val result = results.next
          System.out.println(result.rowToString)
          cont += 1
        }
      }
      System.out.println("Number of rows: " + cont)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      try {
        client.shutdown()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /**
   * 将一行所有字段的数据格式化成String拼接，然后返回
   *
   * @param row 行对象
   * @return
   */
  def printRow(row: Row): String = {
    var res = ""
    for (i <- 0 until row.getArity) {
      res += (row.getField(i) + " | ")
    }
    res
  }

  /**
   * 清空表内容
   *
   * @param tableName
   */
  def clearTable(tableName: String): Unit = {
    val table = client.openTable(tableName)
    val rowsList = readTable(tableName)
    val primaryKey = table.getSchema.getPrimaryKeyColumns.get(0).getName
    val deletes = new util.ArrayList[Delete]
    import scala.collection.JavaConversions._
    for (row <- rowsList) {
      val d = table.newDelete
      val tp = getRowsPositionType(0, row)
      val partialRow = d.getRow
      TableUtil.valueToRow(partialRow, tp, primaryKey, row.getField(0))
      deletes.add(d)
    }
    import scala.collection.JavaConversions._
    for (d <- deletes) {
      session.apply(d)
    }
    logger.info("SUCCESS: The table has been emptied successfully")
  }

  /**
   * 返回表的所有字段的字段名
   *
   * @param table
   * @return
   */
  def getNamesOfColumns(table: KuduTable): Array[String] = {
    val columns = table.getSchema.getColumns
    val columnsNames = new util.ArrayList[String]
    import scala.collection.JavaConversions._
    for (schema <- columns) {
      columnsNames.add(schema.getName)
    }
    var array = new Array[String](columnsNames.size)
    array = columnsNames.toArray(array)
    array
  }

  /**
   * 判断提供的表与原表字段是否相同
   *
   * @param tableNames
   * @param providedNames
   * @return
   */
  def checkNamesOfColumns(tableNames: Array[String], providedNames: Array[String]): Boolean = {
    var res = false
    if (tableNames.length != providedNames.length) res = false
    else {
      for (i <- 0 until tableNames.length) {
        res = {
          if (tableNames(i) == providedNames(i)) true
          else false
        }
      }
    }
    if (!res) throw new KuduTableException("ERROR: The table column names and the provided column names don't match")
    res
  }

  /**
   * 向表中添加一行数据
   * @param table 插入的kudu表对象
   * @param row 插入的行对象
   * @param fieldsNames 表字段名称集
   * @return 返回操作结果响应对象
   */
  def insert(table: KuduTable, row: Row, fieldsNames: Array[String]): OperationResponse = {
    var insert:Insert = null
    try insert = table.newInsert
    catch {
      case e: NullPointerException =>
        throw new NullPointerException("Error encountered at opening/creating table")
    }
    for (index <- 0 until row.getArity) {
      val partialRow = insert.getRow
      if (row.getField(index) != null) {
        val tp = getRowsPositionType(index, row)
        TableUtil.valueToRow(partialRow, tp, fieldsNames(index), row.getField(index))
      } else partialRow.setNull(index)//如果对应字段没有数据，者写null
    }
    session.apply(insert)
  }

  /**
   * 如果该行数据的主键存在就更新数据，否则插入数据
   *
   * @param table
   * @param row
   * @param fieldsNames
   * @return
   */
  def upsert(table: KuduTable, row: Row, fieldsNames: Array[String]): OperationResponse = {
    var upsert:Upsert = null
    try upsert = table.newUpsert
    catch {
      case e: NullPointerException =>
        throw new NullPointerException("Error encountered at opening/creating table")
    }
    for (index <- 0 until row.getArity) {
      val partialRow = upsert.getRow
      if (row.getField(index) != null) {
        val tp = getRowsPositionType(index, row)
        TableUtil.valueToRow(partialRow, tp, fieldsNames(index), row.getField(index))
      } else partialRow.setNull(index)
    }
    session.apply(upsert)
  }

  /**
   * 更新行数据
   *
   * @param table
   * @param row
   * @param fieldsNames
   * @return
   */
  def update(table: KuduTable, row: Row, fieldsNames: Array[String]): OperationResponse = {
    var update:Update = null
    try update = table.newUpdate
    catch {
      case e: NullPointerException =>
        throw new NullPointerException("Error encountered at opening/creating table")
    }
    for (index <- 0 until row.getArity) {
      val partialRow = update.getRow
      if (row.getField(index) != null) {
        val tp = getRowsPositionType(index, row)
        TableUtil.valueToRow(partialRow, tp, fieldsNames(index), row.getField(index))
      }
      else partialRow.setNull(index)
    }
    session.apply(update)
  }

  /**
   * 删除行数据
   *
   * @param table
   * @param row
   * @param fieldsNames
   * @param keyColumnCount
   * @return
   */
  def delete(table: KuduTable, row: Row, fieldsNames: Array[String], keyColumnCount: Integer): OperationResponse = {
    var delete:Delete = null
    try delete = table.newDelete
    catch {
      case e: NullPointerException =>
        throw new NullPointerException("Error encountered at opening/creating table")
    }
    val partialRow = delete.getRow
    for (index <- 0 until keyColumnCount) {
      val tp = getRowsPositionType(index, row)
      TableUtil.valueToRow(partialRow, tp, fieldsNames(index), row.getField(index))
    }
    session.apply(delete)
  }

  /**
   * 返回客户端对象
   *
   * @return Kudu client
   */
  def getClient: KuduClient = client
}
