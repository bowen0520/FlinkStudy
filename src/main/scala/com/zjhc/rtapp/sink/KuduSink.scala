package com.zjhc.rtapp.sink

import com.alibaba.fastjson.JSON
import com.zjhc.rtapp.utils.exceptions.KuduClientException
import com.zjhc.rtapp.utils.{MyJedisPool, PropertiesUtil, Util}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduTable
import org.apache.kudu.client.OperationResponse
import org.apache.log4j.Logger
import redis.clients.jedis.JedisCluster
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent._

import com.zjhc.rtapp.kafka.Constants._
import com.zjhc.rtapp.kafka.Constants.SINK_COUNT_INSERT
import com.zjhc.rtapp.kafka.KafkaToKudu.changeColType
import com.zjhc.rtapp.utils.PropertiesUtil.insertJobReject


object KuduSink { // LOG4J
  private val logger = Logger.getLogger(classOf[KuduSink])
}

class KuduSink(host: String, tableName: String) extends RichSinkFunction[KuduSinkVO] {

  if (host == null || host.isEmpty) {
    throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)")
  } else if (tableName == null || tableName.isEmpty()) {
    throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)")
  }

  private var jobID:String = null
  private var fieldsNames:Array[String] = null
  private var opColumnIndex:Int = null
  private var keyColumnCount:Int = null
  private var loadType:Int = null
  private var groupTime:Long = 0L
  private var util:Util = null
  private var delayTime:Int = 0
  private var runTime:Long = 0L
  //Kudu variables
  private var table:KuduTable = null
  private var jedisCluster:JedisCluster = null

  override def open(parameters: Configuration): Unit = {
    this.jedisCluster = MyJedisPool.getObject
    super.open(parameters)
  }

  override def close(): Unit = {
    if (jedisCluster != null) jedisCluster.close()
    util.getClient.close()
    super.close()
  }

  def this(jobId: String, host: String, tableName: String,
           fieldsNames: Array[String], opColumnIndex: Integer,
           keyColumnCount: Integer, loadType: Integer, groupTime: Long){
    this(host,tableName)
    this.jobID = jobId
    this.fieldsNames = fieldsNames
    this.opColumnIndex = opColumnIndex
    this.keyColumnCount = keyColumnCount
    this.loadType = loadType
    this.groupTime = groupTime
  }

  def this(host: String, tableName: String,
           fieldsNames: Array[String], opColumnIndex: Integer,
           keyColumnCount: Integer, loadType: Integer){
    this(host,tableName)
    this.fieldsNames = fieldsNames
    this.opColumnIndex = opColumnIndex
    this.keyColumnCount = keyColumnCount
    this.loadType = loadType
  }

  override def invoke(sinkVO: KuduSinkVO): Unit = {
    val row = sinkVO.getRow
    if (sinkVO.getEtlColumn) if (sinkVO.getTypeName == "unixtime_micros") {
      val etlTime = (System.currentTimeMillis / 1000 * 1000 + 8 * 3600 * 1000) * 1000
      row.setField(sinkVO.getColumnCount, changeColType(sinkVO.getTypeName, String.valueOf(etlTime).trim))
    }
    else {
      val df = new SimpleDateFormat("yyyyMMddHHmmss") //设置日期格式
      row.setField(sinkVO.getColumnCount, changeColType(sinkVO.getTypeName, df.format(new Date).trim))
    }
    val value = sinkVO.getValue
    if (!(row == new Row(0))) { // Establish connection with Kudu
      if (this.util == null) this.util = new Util(host)
      this.table = this.util.useTable(tableName, fieldsNames, row)
      if (1 == loadType) if (row.getField(opColumnIndex) == INSERT) erroLog(util.insert(table, row, fieldsNames), value, INSERT, sinkVO.getMap)
      else if (row.getField(opColumnIndex) == UPDATE) erroLog(util.update(table, row, fieldsNames), value, UPDATE, sinkVO.getMap)
      else if (row.getField(opColumnIndex) == DELETE) erroLog(util.update(table, row, fieldsNames), value, DELETE, sinkVO.getMap)
      else erroLog(util.insert(table, row, fieldsNames), value, INSERT, sinkVO.getMap)
    }
  }

  private def setDaleyTime(map: util.Map[String, AnyRef]): Unit = {
    val etlTime = new Date
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dmlExecuteTimestamp = String.valueOf(map.get(PropertiesUtil.getProperty("kudu.status.dml_time_col")))
    val dml = sdf.parse(dmlExecuteTimestamp)
    if (runTime == 0) runTime = etlTime.getTime
    val differenceTime = etlTime.getTime - runTime
    val delay:Int = ((etlTime.getTime - dml.getTime) / 1000).toFloat.round
    if (delay > delayTime || differenceTime > groupTime * 60000) {
      if (differenceTime > groupTime * 60000) runTime = etlTime.getTime
      this.delayTime = delay
      val delayVO = new DelayVO
      delayVO.setJobId(this.jobID.toLong)
      delayVO.setTableName(this.tableName)
      delayVO.setDmlExecuteTimestamp(dmlExecuteTimestamp)
      delayVO.setEtlTime(sdf.format(etlTime))
      delayVO.setDelayTime(delayTime.toLong)
      jedisCluster.set(SINK_DELAY + this.jobID, JSON.toJSONString(delayVO))
    }
  }

  private def erroLog(response: OperationResponse, value: String, opType: String, map: util.Map[String, AnyRef]): Unit = {
    if (response.hasRowError) {
      var `type` = "3"
      if ("Already present: key already present (error 0)" == response.getRowError.getErrorStatus.toString) `type` = "2"
      insertJobReject(this.jobID, this.tableName, `type`, value, response.getRowError.getErrorStatus.toString)
    }
    else {
      if (INSERT == opType) jedisCluster.incr(SINK_COUNT_INSERT + jobID)
      else if (UPDATE == opType) jedisCluster.incr(SINK_COUNT_UPDATE + jobID)
      else if (DELETE == opType) jedisCluster.incr(SINK_COUNT_DELETE + jobID)
      setDaleyTime(map)
    }
  }
}