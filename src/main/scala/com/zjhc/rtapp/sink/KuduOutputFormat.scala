package com.zjhc.rtapp.sink

import org.apache.flink.types.Row
import com.zjhc.rtapp.utils.Util
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.kudu.client.KuduTable
import org.apache.log4j.Logger


object KuduOutputFormat {
  //Modes
  val CREATE = 1
  val APPEND = 2
  val OVERRIDE = 3
  //LOG4J
  private val logger = Logger.getLogger(classOf[KuduOutputFormat])
}

class KuduOutputFormat(host: String, tableName: String, var fieldsNames: Array[String], var tableMode: Integer)
  extends RichOutputFormat[Row] {
  private var util:Util = null
  //Kudu variables
  private var table:KuduTable = null

  if (tableMode == null ||
    (tableMode != KuduOutputFormat.CREATE
        && tableMode != KuduOutputFormat.APPEND
        && tableMode != KuduOutputFormat.OVERRIDE)) {
    throw new IllegalArgumentException("ERROR: Param \"tableMode\" not valid (null or empty)")
  } else if (tableMode == KuduOutputFormat.CREATE && (fieldsNames == null || fieldsNames.length == 0)){
    throw new IllegalArgumentException("ERROR: Missing param \"fieldNames\". Can't create a table without column names")
  } else if (host == null || host.isEmpty) {
    throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)")
  } else if (tableName == null || tableName.isEmpty) {
    throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)")
  }

  /**
   *
   * @param host
   * @param tableName
   * @param tableMode
   */
  def this(host: String, tableName: String, tableMode: Integer){
    this(host,tableName,null,tableMode)
  }

  override def configure(configuration: Configuration): Unit = {
  }

  /**
   * 打开kudu并拿到表对象
   * @param i
   * @param i1
   */
  override def open(i: Int, i1: Int): Unit = {
    this.util = new Util(host)
    if (util.getClient.tableExists(tableName)) {
      KuduOutputFormat.logger.info("Mode is CREATE and table already exist. Changed mode to APPEND. Warning, parallelism may be less efficient")
      tableMode = KuduOutputFormat.APPEND
    }
    // Case APPEND (or OVERRIDE), with builder without column names, because otherwise it throws a NullPointerException
    if (tableMode == KuduOutputFormat.APPEND || tableMode == KuduOutputFormat.OVERRIDE) {
      this.table = util.useTable(tableName, tableMode)
      if (fieldsNames == null || fieldsNames.length == 0) fieldsNames = util.getNamesOfColumns(table)
      else {
        util.checkNamesOfColumns(util.getNamesOfColumns(this.table), fieldsNames)
      }
    }
  }

  /**
   * 关闭客户端对象
   */
  override def close(): Unit = {
    this.util.getClient.close()
  }

  /**
   * 向kudu表中写入数据
   * @param row
   */
  override def writeRecord(row: Row): Unit = {
    if (tableMode == KuduOutputFormat.CREATE && !util.getClient.tableExists(tableName)) {
      createTable(util, tableName, fieldsNames, row)
    } else this.table = util.getClient.openTable(tableName)
    if (table != null) util.insert(table, row, fieldsNames)
  }

  /**
   * 创建表
   * @param util
   * @param tableName
   * @param fieldsNames
   * @param row
   */
  private def createTable(util: Util, tableName: String, fieldsNames: Array[String], row: Row): Unit = {
    synchronized {
      this.table = util.useTable(tableName, fieldsNames, row)
      if (fieldsNames == null || fieldsNames.length == 0) this.fieldsNames = util.getNamesOfColumns(table)
      else util.checkNamesOfColumns(util.getNamesOfColumns(this.table), fieldsNames)
    }
  }
}

