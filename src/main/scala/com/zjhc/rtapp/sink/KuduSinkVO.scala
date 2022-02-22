package com.zjhc.rtapp.sink

import java.util

import org.apache.flink.types.Row

class KuduSinkVO {
  private var row:Row = null
  private var value:String = null
  private var etlColumn:Boolean = false
  private var typeName:String = null
  private var columnCount:Int = null
  private var map:util.Map[String,AnyRef] = null


  def getRow: Row = row

  def setRow(row: Row): Unit = {
    this.row = row
  }

  def getValue: String = value

  def setValue(value: String): Unit = {
    this.value = value
  }

  def getEtlColumn: Boolean = etlColumn

  def setEtlColumn(etlColumn: Boolean): Unit = {
    this.etlColumn = etlColumn
  }

  def getTypeName: String = typeName

  def setTypeName(typeName: String): Unit = {
    this.typeName = typeName
  }

  def getColumnCount: Integer = columnCount

  def setColumnCount(columnCount: Integer): Unit = {
    this.columnCount = columnCount
  }

  def getMap: util.Map[String, AnyRef] = map

  def setMap(map: util.Map[String, AnyRef]): Unit = {
    this.map = map
  }
}
