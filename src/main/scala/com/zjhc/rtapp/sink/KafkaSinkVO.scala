package com.zjhc.rtapp.sink

class KafkaSinkVO {
  private var opType:String = null
  private var jobId:String = null
  private var dmlExecuteTimestamp:String = null
  private var groupTime = 0L
  private var tableName:String = null
  private var value:String = null


  def getOpType: String = opType

  def setOpType(opType: String): Unit = {
    this.opType = opType
  }

  def getJobId: String = jobId

  def setJobId(jobId: String): Unit = {
    this.jobId = jobId
  }

  def getDmlExecuteTimestamp: String = dmlExecuteTimestamp

  def setDmlExecuteTimestamp(dmlExecuteTimestamp: String): Unit = {
    this.dmlExecuteTimestamp = dmlExecuteTimestamp
  }

  def getGroupTime: Long = groupTime

  def setGroupTime(groupTime: Long): Unit = {
    this.groupTime = groupTime
  }

  def getTableName: String = tableName

  def setTableName(tableName: String): Unit = {
    this.tableName = tableName
  }

  def getValue: String = value

  def setValue(value: String): Unit = {
    this.value = value
  }
}
