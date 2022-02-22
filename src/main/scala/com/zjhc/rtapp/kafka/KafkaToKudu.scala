package com.zjhc.rtapp.kafka

import com.zjhc.rtapp.sink.KuduSink
import com.zjhc.rtapp.sink.KuduSinkVO
import com.zjhc.rtapp.utils.PropertiesUtil
import com.zjhc.rtapp.utils.RedisUtil
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.types.Row
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduSession
import org.apache.kudu.client.KuduTable
import java.text.SimpleDateFormat
import java.sql.Connection
import java.sql.ResultSet
import java.util
import java.util._

import com.zjhc.rtapp.utils.PropertiesUtil.insertJobReject
import org.apache.flink.shaded.zookeeper.org.apache.zookeeper.ZooDefs.OpCode.sasl


/**
 * Created by liang on 2018/12/3.
 */
object KafkaToKudu {
  private[kafka] var conn = PropertiesUtil.getDbConnection
  private[kafka] val bufferTimeout = PropertiesUtil.getProperty("flink.env.buffer_timeout").toInt
  private[kafka] val restartAttempts = PropertiesUtil.getProperty("flink.env.restart_attempts").toInt
  private[kafka] val delayBetweenAttempts = PropertiesUtil.getProperty("flink.env.delay_between_attempts").toInt
  private[kafka] val checkpointInterval = PropertiesUtil.getProperty("flink.env.checkpoint_interval").toInt
  private[kafka] val autoCommit = PropertiesUtil.getProperty("kafka.enable.auto.commit")
  private[kafka] val offsetReset = PropertiesUtil.getProperty("kafka.auto.offset.reset")
  private[kafka] val checkpointTimeout = PropertiesUtil.getProperty("flink.env.checkpoint_timeout").toInt
  private[kafka] val maxConcurrentCheckpoints = PropertiesUtil.getProperty("flink.env.max_concurrent_checkpoints").toInt
  private[kafka] val autoCommitInterval = PropertiesUtil.getProperty("kafka.auto.commit.interval.ms")

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    var tableName:String = null
    var jobName:String = null
    var KUDU_MASTER:String = null
    var columnListSource:String = null
    var keyColumn:String = null
    var opColumn:String = null
    var filterColumn:String = null
    var filterValues:String = null
    var etlColumn:String = null
    var sql:String = null
    var groupTime:Long = null
    val kafka_brokers:String = null
    val zookeeper_connect:String = null
    var kudu_master:String = null
    var topicName:String = null
    var loadType:Int = null
    var splitString:String = null
    var authType:String = null
    var saslJaasConfig:String = null
    var sslKeystoreLocation:String = null
    var sslKeystorePassword:String = null
    var sslKeyPassword:String = null
    var sslTruststoreLocation:String = null
    var sslTruststorePassword:String = null
    val parameterMap = new util.HashMap[String, String]
    // 解析参数
    val parameterTool = ParameterTool.fromArgs(args)
    if (parameterTool.getNumberOfParameters < 1) {
      System.out.println("Missing parameters!")
      System.out.println("\nUsage: --job_id <job id> ")
      return
    }
    val jobId = parameterTool.getRequired("job_id")
    if (conn != null) if (conn.isClosed) conn = PropertiesUtil.getDbConnection
    sql = "select j.*,s.kafka_brokers,s.zookeeper_connect,t.kudu_master,s.auth_type,s.sasl_jaas_config,s.ssl_keystore_location,s.ssl_keystore_password,s.ssl_key_password,s.ssl_truststore_location,s.ssl_truststore_password" + " from JOB_CONFIG j,SOURCE_SERVER_CONFIG s,TARGET_SERVER_CONFIG t" + " where j.source_server_id=s.source_server_id and j.target_server_id=t.target_server_id" + " and j.job_id = " + jobId
    val resultSet = PropertiesUtil.getData(sql)
    while (resultSet.next) {
      tableName = resultSet.getString("table_name")
      jobName = resultSet.getString("job_name")
      columnListSource = resultSet.getString("column_list_source")
      keyColumn = resultSet.getString("key_column")
      opColumn = resultSet.getString("op_column")
      filterColumn = resultSet.getString("filter_column")
      filterValues = resultSet.getString("filter_values")
      etlColumn = resultSet.getString("etl_column")
      groupTime = resultSet.getLong("group_Time")
      kudu_master = resultSet.getString("kudu_master")
      topicName = resultSet.getString("topic_name")
      splitString = resultSet.getString("split_string")
      loadType = resultSet.getInt("load_type")
      authType = resultSet.getString("auth_type")
      saslJaasConfig = resultSet.getString("sasl_jaas_config")
      sslKeystoreLocation = resultSet.getString("ssl_keystore_location")
      sslKeystorePassword = resultSet.getString("ssl_keystore_password")
      sslKeyPassword = resultSet.getString("ssl_key_password")
      sslTruststoreLocation = resultSet.getString("ssl_truststore_location")
      sslTruststorePassword = resultSet.getString("ssl_truststore_password")

      parameterMap.put("bootstrap.servers", resultSet.getString("kafka_brokers"))
      parameterMap.put("kudu.master", kudu_master)
      parameterMap.put("zookeeper.connect", resultSet.getString("zookeeper_connect"))
      parameterMap.put("group.id", resultSet.getString("group_id"))
      parameterMap.put("topic", topicName)
      parameterMap.put("enable.auto.commit", autoCommit)
      parameterMap.put("auto.commit.interval.ms", autoCommitInterval)

      if (authType == "1") {
        parameterMap.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
        parameterMap.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
        parameterMap.put("sasl.jaas.config", saslJaasConfig)
      }
      else if (authType == "2") {
        parameterMap.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
        parameterMap.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
        parameterMap.put("sasl.jaas.config", saslJaasConfig)
        parameterMap.put("ssl.keystore.location", sslKeystoreLocation)
        parameterMap.put("ssl.keystore.password", sslKeystorePassword)
        parameterMap.put("ssl.key.password", sslKeyPassword)
        parameterMap.put("ssl.truststore.location", sslTruststoreLocation)
        parameterMap.put("ssl.truststore.password", sslTruststorePassword)
      }
      else if (authType == "3") {
        parameterMap.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
        parameterMap.put(SaslConfigs.SASL_MECHANISM, "GSSAPI")
        parameterMap.put("sasl.jaas.config", saslJaasConfig)
      }
    }
    // 获取StreamExecutionEnvironment。
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.setBufferTimeout(bufferTimeout)
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts))
    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(parameterMap))

    val myConsumer = new FlinkKafkaConsumer010[String](topicName, new SimpleStringSchema, ParameterTool.fromMap(parameterMap).getProperties)
    myConsumer.setStartFromGroupOffsets
    val sourceStream = env.addSource(myConsumer)
    KUDU_MASTER = kudu_master //"192.168.1.11";

    val client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build
    val table = if (3 == loadType) client.openTable(tableName.split("\\|")(0))
    else client.openTable(tableName)
    val session = client.newSession
    val columnCount = table.getSchema.getColumnCount
    val splitString2 = splitString
    val columnNames = new Array[String](columnCount)
    val typeNames = new Array[String](columnCount)
    for (i <- 0 until columnCount) {
      columnNames(i) = table.getSchema.getColumnByIndex(i).getName
      typeNames(i) = table.getSchema.getColumnByIndex(i).getType.getName
    }
    session.flush
    //获取主键个数，默认为1
    var keyColumnCount = 0
    if (keyColumn.length > 0) {
      val keyColumnNames = keyColumn.split(",")
      for (i <- 0 until keyColumnNames.length) {
        if (getIndex(columnNames, keyColumnNames(i)) >= 0) keyColumnCount += 1
      }
    }
    else keyColumnCount = 1
    //获取opColumn字段位置，默认为0
    var opColumnIndex:Int = null
    if (opColumn.length > 0) opColumnIndex = getIndex(columnNames, opColumn)
    else opColumnIndex = 0
    if ((etlColumn != null) && (etlColumn.length != 0)) columnListSource = columnListSource + ',' + etlColumn
    //若columnNamesSource字段为空，默认为columnNames值
    var columnNamesSource:Array[String] = null
    if (columnListSource.length > 0) columnNamesSource = columnListSource.split(",")
    else columnNamesSource = columnNames
    //System.out.println(getIndex(columnNamesSource, "order_item_id"));
    val filterColumn2 = filterColumn
    val filterValues2 = filterValues
    val etlColumn2 = etlColumn
    val tableNameList = util.Arrays.asList(tableName.split("\\|"))
    //过滤和转换
    val stream2 = sourceStream.filter(new FilterFunction[String]() {
      @throws[Exception]
      override def filter(value: String): Boolean = { //空行过滤
        if (StringUtils.isBlank(value)) return false
        else if (true) {
          val args = value.split(splitString2, -1)
          if ((etlColumn2 != null) && (etlColumn2.length != 0)) if (columnCount != args.length + 1) {
            import scala.collection.JavaConversions._
            for (tableName:String <- tableNameList) {
              insertJobReject(jobId, tableName, "1", value, "源与目标字段个数不匹配")
              return false
            }
          }
          else if (columnCount != args.length) {
            import scala.collection.JavaConversions._
            for (tableName:String <- tableNameList) {
              insertJobReject(jobId, tableName, "1", value, "源与目标字段个数不匹配")
              return false
            }
          }
        }
        else { //如果过滤字段或过滤值为空，返回True(不过滤)
          if ((filterColumn2 == null) || (filterValues2 == null) || (filterColumn2.length == 0) || (filterValues2.length == 0)) return true
          else { //按过滤字段和过滤值过滤
            val arrValues = filterValues2.split(splitString2, -1)
            val filterIndex = getIndex(columnNamesSource, filterColumn2)
            if ((filterIndex eq -(1)) || (getIndex(arrValues, args(filterIndex)) >= 0)) return StringUtils.isNotBlank(value)
            else return false
          }
        }
        true
        // return StringUtils.isNotBlank(value);
      }
    }).map(new MapFunction[String, KuduSinkVO]() {
      override def map(value: String): KuduSinkVO = {
        val sinkVO = new KuduSinkVO
        try { //将源字段和目标字段进行匹配和类型转换
          val res = new Row(columnCount)
          //Row res = new Row(args.length);
          var count = columnCount
          if ((etlColumn2 != null) && (etlColumn2.length != 0)) {
            sinkVO.setColumnCount(columnCount - 1)
            sinkVO.setEtlColumn(true)
            sinkVO.setTypeName(typeNames(getIndex(columnNames, etlColumn2)))
            count = columnCount - 1
          }
          val args = value.split(splitString2, -1)
          val map = new util.LinkedHashMap[String, AnyRef]
          for (i <- 0 until count) {
            val columnIndex = getIndex(columnNamesSource, columnNames(i))
            if (columnIndex ne -1) {
              res.setField(i, changeColType(typeNames(i), args(columnIndex).trim))
              map.put(columnNames(i), args(columnIndex).trim)
            }
          }
          sinkVO.setMap(map)
          sinkVO.setRow(res)
          sinkVO.setValue(value)
          return sinkVO
        } catch {
          case e: Exception =>
            import scala.collection.JavaConversions._
            for (tableName:String <- tableNameList) {
              insertJobReject(jobId, tableName, "1", value, e.getMessage)
            }
        }
        sinkVO.setValue(value)
        sinkVO.setRow(new Row(0))
        sinkVO
      }
    }).setBufferTimeout(bufferTimeout)
    stream2.print
    if (3 == loadType) {
      val tableNames = tableName.split("\\|")
      for (s <- tableNames) {
        stream2.addSink(new KuduSink(jobId, KUDU_MASTER, s, columnNames, opColumnIndex, keyColumnCount, loadType, groupTime))
      }
    }
    else stream2.addSink(new KuduSink(jobId, KUDU_MASTER, tableName, columnNames, opColumnIndex, keyColumnCount, loadType, groupTime))

    env.execute("Job ID:" + jobId + "（" + jobName + "）")
  }

  def getIndex(arr: Array[String], value: String): Int = {
    for (i <- 0 until arr.length) {
      if (arr(i) == value) return i
    }
    -1
  }

  def changeColType(colType: String, col: String): Any = {
    var value = new Any
    val obj = null
    if (colType == "int8") { //value=col.equals("")?0:Byte.parseByte(col);
      value = if (col == "") null
      else col.toByte
    }
    else if (colType == "int16") { //value=col.equals("")?0:Short.parseShort(col);
      value = if (col == "") null
      else col.toShort
    }
    else if (colType == "int32") { //value=col.equals("")?0:Integer.valueOf(col);
      value = if (col == "") null
      else col.toInt
    }
    else if (colType == "int64") { //value=col.equals("")?0:Long.parseLong(col);
      value = if (col == "") null
      else col.toLong
    }
    else if (colType == "double") { //value=col.equals("")?0:Double.parseDouble(col);
      value = if (col == "") null
      else col.toDouble
    }
    else if (colType == "float") { //value=col.equals("")?0:Float.parseFloat(col);
      value = if (col == "") null
      else col.toFloat
    }
    else if (colType == "unixtime_micros") {
      value = if (col == "") null
      else col.toLong
      //value=col.equals("")?Timestamp.valueOf("1900-01-01 00:00:00"):Timestamp.valueOf(col);
    }
    else value = col
    value
  }
}

