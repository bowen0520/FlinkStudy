package com.zjhc.rtapp.kafka

import com.alibaba.fastjson.JSON
import com.zjhc.rtapp.sink.DelayVO
import com.zjhc.rtapp.sink.KafkaSink
import com.zjhc.rtapp.sink.KafkaSinkVO
import com.zjhc.rtapp.utils.PropertiesUtil
import com.zjhc.rtapp.utils.RedisUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.config.SaslConfigs
import java.sql.Connection
import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util._

import com.zjhc.rtapp.kafka.Constants._
import com.zjhc.rtapp.utils.PropertiesUtil.insertJobReject


/**
 * Created by liang on 2018/12/3.
 */
object KafkaToKafka {
  //默认配置
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

  var ACL_TYPE = "1"
  private[kafka] val JSON_TYPE = "0"
  private[kafka] val OP_TYPE_ONE = "1"
  private[kafka] val OP_TYPE_TWO = "2"

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    //设置配置
    var tableName:String = null
    var jobName:String = null
    var columnListSource:String = null
    var keyColumn:String = null
    var authType:String = null
    var targetAuthType:String = null
    var targetUserName:String = null
    var targetPassword:String = null
    var targetConnect:String = null
    var targetBroker:String = null
    var targetTopic:String = null
    var targetInfoType:String = null
    var targetOpType:String = null
    var targetColumnType:String = null
    var targetColumn:String = null
    var targetSplit:String = null
    var filterColumn:String = null
    var filterValues:String = null
    var etlColumn:String = null
    var sql:String = null
    var opColumn:String = null
    var opTimeColumn:String = null
    var groupTime:Long = null
    var topicName:String = null
    var loadType:Int = null
    var userName:String = null
    var password:String = null
    var splitString:String = null
    var partitionNum:Int = null
    var saslJaasConfig:String = null
    var sslKeystoreLocation:String = null
    var sslKeystorePassword:String = null
    var sslKeyPassword:String = null
    var sslTruststoreLocation:String = null
    var sslTruststorePassword:String = null
    var targetSaslJaasConfig:String = null
    var targetSslKeystoreLocation:String = null
    var targetSslKeystorePassword:String = null
    var targetSslKeyPassword:String = null
    var targetSslTruststoreLocation:String = null
    var targetSslTruststorePassword:String = null

    val parameterMap = new util.HashMap[String, String](9)
    // 解析参数
    val parameterTool = ParameterTool.fromArgs(args)
    if (parameterTool.getNumberOfParameters < 1) {
      System.out.println("Missing parameters!")
      System.out.println("\nUsage: --job_id <job id> ")
      return
    }

    val jobId = parameterTool.getRequired("job_id")
    if (conn != null && conn.isClosed) {
      conn = PropertiesUtil.getDbConnection
    }
    //获取配置
    sql = "select j.*,s.kafka_brokers,s.zookeeper_connect,t.kudu_master,s.auth_type,s.user_name,s.password,s.sasl_jaas_config,s.ssl_keystore_location,s.ssl_keystore_password,s.ssl_key_password,s.ssl_truststore_location,s.ssl_truststore_password" + ",t.auth_type as targetAuthType,t.user_name as targetUserName,t.password as targetPassword," + "t.zookeeper_connect as targetConnect,t.kafka_broker as targetBroker,t.sasl_jaas_config as target_sasl_jaas_config,t.ssl_keystore_location as target_ssl_keystore_location ,t.ssl_keystore_password as target_ssl_keystore_password,t.ssl_key_password as target_ssl_key_password,t.ssl_truststore_location as target_ssl_truststore_location,t.ssl_truststore_password as target_ssl_truststore_password " + "from JOB_CONFIG j,SOURCE_SERVER_CONFIG s,TARGET_SERVER_CONFIG t" + " where j.source_server_id=s.source_server_id and j.target_server_id=t.target_server_id" + " and j.job_id = " + jobId
    val resultSet = PropertiesUtil.getData(sql)
    while (resultSet.next) {
      tableName = resultSet.getString("table_name")
      jobName = resultSet.getString("job_name")
      columnListSource = resultSet.getString("column_list_source")
      keyColumn = resultSet.getString("key_column")
      authType = resultSet.getString("auth_type")
      filterColumn = resultSet.getString("filter_column")
      filterValues = resultSet.getString("filter_values")
      etlColumn = resultSet.getString("etl_column")
      userName = resultSet.getString("user_name")
      password = resultSet.getString("password")
      opColumn = resultSet.getString("op_column")
      targetTopic = resultSet.getString("target_topic")
      targetSplit = resultSet.getString("target_split")
      targetColumn = resultSet.getString("target_column")
      targetColumnType = resultSet.getString("target_column_type")
      targetAuthType = resultSet.getString("targetAuthType")
      targetUserName = resultSet.getString("targetUserName")
      targetPassword = resultSet.getString("targetPassword")
      targetBroker = resultSet.getString("targetBroker")
      targetInfoType = resultSet.getString("target_info_type")
      targetOpType = resultSet.getString("op_type")
      opTimeColumn = resultSet.getString("op_time_column")
      groupTime = resultSet.getLong("group_Time")
      topicName = resultSet.getString("topic_name")
      splitString = resultSet.getString("split_string")
      targetConnect = resultSet.getString("targetConnect")
      partitionNum = resultSet.getInt("partition_num")
      loadType = resultSet.getInt("load_type")
      saslJaasConfig = resultSet.getString("sasl_jaas_config")
      sslKeystoreLocation = resultSet.getString("ssl_keystore_location")
      sslKeystorePassword = resultSet.getString("ssl_keystore_password")
      sslKeyPassword = resultSet.getString("ssl_key_password")
      sslTruststoreLocation = resultSet.getString("ssl_truststore_location")
      sslTruststorePassword = resultSet.getString("ssl_truststore_password")
      targetSaslJaasConfig = resultSet.getString("target_sasl_jaas_config")
      targetSslKeystoreLocation = resultSet.getString("target_ssl_keystore_location")
      targetSslKeystorePassword = resultSet.getString("target_ssl_keystore_password")
      targetSslKeyPassword = resultSet.getString("target_ssl_key_password")
      targetSslTruststoreLocation = resultSet.getString("target_ssl_truststore_location")
      targetSslTruststorePassword = resultSet.getString("target_ssl_truststore_password")
      //加载配置
      parameterMap.put("bootstrap.servers", resultSet.getString("kafka_brokers"))
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
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //加载配置
    env.getConfig.disableSysoutLogging
    env.setBufferTimeout(bufferTimeout)
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts))
    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(parameterMap))
    //获取数据源
    val sourceStream = env.addSource(new FlinkKafkaConsumer010[String](topicName, new SimpleStringSchema, ParameterTool.fromMap(parameterMap).getProperties))

    var keyColumnCount = 0
    if (keyColumn.length > 0) {
      val keyColumnNames = keyColumn.split(",")
      keyColumnCount = keyColumnNames.length
    }
    else keyColumnCount = 0
    val keyColumn2 = keyColumn
    val columnCount = columnListSource.length
    val splitString2 = splitString
    val targetSplit2 = targetSplit

    var columnNamesSource:Array[String] = null
    var columnNamesTarget:Array[String] = null

    columnNamesSource = StringUtils.split(columnListSource, ",")
    if (OP_TYPE_TWO == targetColumnType) columnNamesTarget = targetColumn.split(",", -1)
    else columnNamesTarget = columnNamesSource
    val targetColumnType2 = targetColumnType

    val opColumnIndex = getIndex(columnNamesSource, opColumn)
    val opTimeIndex = getIndex(columnNamesSource, opTimeColumn)
    val keyIndex = getIndex(columnNamesSource, keyColumn)
    val filterColumn2 = filterColumn
    val filterValues2 = filterValues
    val etlColumn2 = etlColumn
    val tableNameList = util.Arrays.asList(tableName.split("\\|"))

    val infoType = targetInfoType
    val table = tableName
    val opType = targetOpType
    val opTime = opTimeColumn
    val groupDate = groupTime

    //数据转换
    val stream2 = sourceStream.filter(new FilterFunction[String]() {
      @throws[Exception]
      override def filter(value: String): Boolean = { //空行过滤
        if (StringUtils.isBlank(value)) StringUtils.isNotBlank(value)
        else {
          val args = value.split(splitString2, -1)
          if (args.length != columnNamesSource.length) {
            import scala.collection.JavaConversions._
            for (tableName:String <- tableNameList) {
              insertJobReject(jobId, tableName, "1", value, "源解析和源配置字段个数不匹配")
            }
            return false
          }
          //如果过滤字段或过滤值为空，返回True(不过滤)
          if ((filterColumn2 == null) || (filterValues2 == null) || (filterColumn2.length == 0) || (filterValues2.length == 0)) true
          else {
            val arrValues = filterValues2.split(splitString2, -1)
            val filterIndex = getIndex(columnNamesSource, filterColumn2)
            if ((filterIndex eq -(1)) || (getIndex(arrValues, args(filterIndex)) >= 0)) StringUtils.isNotBlank(value)
            else false
          }
        }
      }
    }).map(new MapFunction[String, KafkaSinkVO]() {
      @throws[Exception]
      override def map(value: String): KafkaSinkVO = {
        val kafkaSinkVO = new KafkaSinkVO
        kafkaSinkVO.setJobId(jobId)
        kafkaSinkVO.setGroupTime(groupDate)
        kafkaSinkVO.setTableName(table)
        if (JSON_TYPE == infoType) {
          val args = value.split(splitString2, -1)
          val res = new StringBuilder("")
          res.append("{")
          for (i <- 0 until columnNamesTarget.length) {
            val columnValue = if (getIndex(columnNamesSource, columnNamesTarget(i)) == -(1)) ""
            else args(getIndex(columnNamesSource, columnNamesTarget(i)))
            val splitStr = if (i == 0) ""
            else ","
            columnNamesTarget(i) match {
              case TABLE =>
                res.append(splitStr + "\"" + TABLE + "\":\"" + table + "\"")

              case OP_TYPE =>
                res.append(splitStr + "\"" + OP_TYPE + "\":\"" + changeOpType(opType, args(opColumnIndex)) + "\"")
                kafkaSinkVO.setOpType(args(opColumnIndex))

              case CURRENT_TS =>
                res.append(splitStr + "\"" + CURRENT_TS + "\":\"" + getNowTime + "\"")

              case PRIMARY_KEY =>
                res.append(splitStr + "\"" + PRIMARY_KEY + "\":" + getPK(keyColumn2))

              case _ =>
                res.append(splitStr + "\"" + columnNamesTarget(i) + "\":\"" + columnValue + "\"")

            }
            if (columnNamesTarget(i) == opTime) kafkaSinkVO.setDmlExecuteTimestamp(args(getIndex(columnNamesSource, columnNamesTarget(i))))
          }
          res.append("}")
          kafkaSinkVO.setValue(res.toString)
        }
        else { //kafkaSinkVO.setValue(value);
          val args = value.split(splitString2, -1)
          val res = new StringBuilder("")
          for (i <- 0 until columnNamesTarget.length) {
            val columnValue = if (getIndex(columnNamesSource, columnNamesTarget(i)) == -(1)) ""
            else args(getIndex(columnNamesSource, columnNamesTarget(i)))
            val splitStr = if (i == 0) ""
            else targetSplit2
            columnNamesTarget(i) match {
              case TABLE =>
                res.append(splitStr + table)

              case OP_TYPE =>
                res.append(splitStr + changeOpType(opType, args(opColumnIndex)))
                kafkaSinkVO.setOpType(args(opColumnIndex))

              case CURRENT_TS =>
                res.append(splitStr + getNowTime)

              case PRIMARY_KEY =>
                res.append(splitStr + keyColumn2)

              case _ =>
                res.append(splitStr + columnValue)

            }
            if (columnNamesTarget(i) == opTime) kafkaSinkVO.setDmlExecuteTimestamp(args(getIndex(columnNamesSource, columnNamesTarget(i))))
          }
          kafkaSinkVO.setValue(res.toString)
        }
        kafkaSinkVO
      }
    }).setBufferTimeout(bufferTimeout)
    //数据输出
    stream2.addSink(new KafkaSink(targetBroker, targetAuthType, targetUserName, targetPassword, targetSaslJaasConfig, targetSslKeystoreLocation, targetSslKeystorePassword, targetSslKeyPassword, targetSslTruststoreLocation, targetSslTruststorePassword, targetConnect, targetTopic, partitionNum))
    //执行job
    env.execute("Job ID:" + jobId + "（" + jobName + "）")
  }

  private def getNowTime = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
    df.format(new Date)
  }

  private def changeOpType(opType: String, value: String): String = {
    if (OP_TYPE_ONE == opType) if (INSERT == value) return I
    else if (UPDATE == value) return U
    else if (DELETE == value) return D
    else if (OP_TYPE_TWO == opType) if (I == value) return INSERT
    else if (U == value) return UPDATE
    else if (D == value) return DELETE
    value
  }

  def getIndex(arr: Array[String], value: String): Int = {
    for (i <- 0 until arr.length) {
      if (arr(i) == value) return i
    }
    -1
  }

  def getPK(keyColumn: String): String = {
    var primaryKeys = "\"\""
    if (keyColumn.length > 0) {
      val argsPK = keyColumn.split(",")
      for (i <- 0 until argsPK.length) {
        if (i == 0) primaryKeys = argsPK(i)
        else primaryKeys = primaryKeys + "\",\"" + argsPK(i)
      }
      if (argsPK.length > 1) primaryKeys = "[\"" + primaryKeys + "\"]"
      else primaryKeys = "\"" + primaryKeys + "\""
    }
    primaryKeys
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

