package com.zjhc.rtapp.sink

class DelayVO() {
  private var jobId = 0L  //jobId
  private var tableName:String = null //表名
  private var dmlExecuteTimestamp:String = null  //
  private var etlTime:String = null
  private var delayTime = 0L

  def getJobId: Long = jobId

  def setJobId(jobId: Long): Unit = {
    this.jobId = jobId
  }

  def getTableName: String = tableName

  def setTableName(tableName: String): Unit = {
    this.tableName = tableName
  }

  def getDmlExecuteTimestamp: String = dmlExecuteTimestamp

  def setDmlExecuteTimestamp(dmlExecuteTimestamp: String): Unit = {
    this.dmlExecuteTimestamp = dmlExecuteTimestamp
  }

  def getEtlTime: String = etlTime

  def setEtlTime(etlTime: String): Unit = {
    this.etlTime = etlTime
  }

  def getDelayTime: Long = delayTime

  def setDelayTime(delayTime: Long): Unit = {
    this.delayTime = delayTime
  }
}
