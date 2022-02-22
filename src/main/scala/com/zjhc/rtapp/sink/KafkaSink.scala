package com.zjhc.rtapp.sink

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.zjhc.rtapp.utils.MyJedisPool
import com.zjhc.rtapp.kafka.Constants
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.config.SaslConfigs
import redis.clients.jedis.JedisCluster

class KafkaSink(brokerList: String, targetAuthType: String,
                targetUserName: String, targetPassword: String,
                targetSaslJaasConfig: String, targetSslKeystoreLocation: String,
                targetSslKeystorePassword: String, targetSslKeyPassword: String,
                targetSslTruststoreLocation: String, targetSslTruststorePassword: String,
                targetConnect: String, topicId: String, partitionNum: Integer)
  extends RichSinkFunction[KafkaSinkVO]{

  private val serialVersionUID = 1L

  private var delayTime = 0L
  private var runTime = 0L

  private var callback:Callback = null

  private var kafkaProducer: KafkaProducer[String, String] = null
  private var jedisCluster:JedisCluster = null

  /**
   * 打开自定义kafka作为sink端所需要打开的资源
   * @param parameters 连接kafka所需要的的配置信息
   */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    this.kafkaProducer = getKafkaSink(brokerList, targetAuthType,
      targetUserName, targetPassword,
      targetSaslJaasConfig, targetSslKeystoreLocation,
      targetSslKeystorePassword, targetSslKeyPassword,
      targetSslTruststoreLocation, targetSslTruststorePassword,
      targetConnect)
    this.jedisCluster = MyJedisPool.getObject
  }

  /**
   * 释放资源
   */
  override def close(): Unit = {
    if (jedisCluster != null) jedisCluster.close()
    super.close()
  }

  /**
   * 自定义将kafka作为sink对象
   * @param sink
   * @param context 上下文对象
   */
  override def invoke(sink: KafkaSinkVO, context: SinkFunction.Context[_]): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String](topicId, partitionNum,
      null, sink.getValue), this.callback)
    this.callback = new Callback() {
      override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
        if (e == null) {
          setRedis(sink.getDmlExecuteTimestamp, sink.getGroupTime,
            sink.getJobId, sink.getTableName, sink.getOpType)
        }
      }
    }
  }

  /**
   * 配置jedisCluster
   * @param dmlExecuteTimestamp
   * @param groupTime
   * @param jobId
   * @param tableName
   * @param opType
   */
  private def setRedis(dmlExecuteTimestamp: String, groupTime: Long,
                       jobId: String, tableName: String,
                       opType: String): Unit = {
    if (Constants.I == opType || Constants.INSERT == opType) {
      jedisCluster.incr(Constants.SINK_COUNT_INSERT + jobId)
    } else if (Constants.U == opType || Constants.UPDATE == opType) {
      jedisCluster.incr(Constants.SINK_COUNT_UPDATE + jobId)
    } else if (Constants.D == opType || Constants.DELETE == opType) {
      jedisCluster.incr(Constants.SINK_COUNT_DELETE + jobId)
    }
    val etlTime = new Date
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var dml = new Date
    try dml = sdf.parse(dmlExecuteTimestamp)
    catch {
      case e: Exception =>
        e.getMessage
    }
    if (runTime == 0) runTime = etlTime.getTime
    val differenceTime = etlTime.getTime - runTime
    val delay = (etlTime.getTime - dml.getTime) / 1000.round
    if (delay > delayTime || differenceTime > groupTime * 60000) {
      if (differenceTime > groupTime * 60000) runTime = etlTime.getTime
      delayTime = delay
      val delayVO = new DelayVO
      delayVO.setJobId(jobId.toLong)
      delayVO.setTableName(tableName)
      delayVO.setDmlExecuteTimestamp(dmlExecuteTimestamp)
      delayVO.setEtlTime(sdf.format(etlTime))
      delayVO.setDelayTime(delayTime)
      jedisCluster.set(Constants.SINK_DELAY + jobId, JSON.toJSONString(delayVO))
    }
  }

  /**
   * 完成配置信息，穿件kafka生产者对象
   * @param brokerList
   * @param targetAuthType
   * @param targetUserName
   * @param targetPassword
   * @param targetSaslJaasConfig
   * @param targetSslKeystoreLocation
   * @param targetSslKeystorePassword
   * @param targetSslKeyPassword
   * @param targetSslTruststoreLocation
   * @param targetSslTruststorePassword
   * @param targetConnect
   * @return
   */
  private def getKafkaSink(brokerList: String, targetAuthType: String,
                           targetUserName: String, targetPassword: String,
                           targetSaslJaasConfig: String, targetSslKeystoreLocation: String,
                           targetSslKeystorePassword: String, targetSslKeyPassword: String,
                           targetSslTruststoreLocation: String, targetSslTruststorePassword: String,
                           targetConnect: String): KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    properties.put("key.serializer.class", "kafka.serializer.StringEncoder")
    properties.put("bootstrap.servers", brokerList)
    properties.put("zookeeper.connect", targetConnect)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    if (targetAuthType == "1") {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
      properties.put("sasl.jaas.config", targetSaslJaasConfig)
    } else if (targetAuthType == "2") {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
      properties.put("sasl.jaas.config", targetSaslJaasConfig)
      properties.put("ssl.keystore.location", targetSslKeystoreLocation)
      properties.put("ssl.keystore.password", targetSslKeystorePassword)
      properties.put("ssl.key.password", targetSslKeyPassword)
      properties.put("ssl.truststore.location", targetSslTruststoreLocation)
      properties.put("ssl.truststore.password", targetSslTruststorePassword)
    } else if (targetAuthType == "3") {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
      properties.put(SaslConfigs.SASL_MECHANISM, "GSSAPI")
      properties.put("sasl.jaas.config", targetSaslJaasConfig)
    }
    new KafkaProducer[String, String](properties)
  }
}
