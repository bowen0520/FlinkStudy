package com.zjhc.rtapp.utils

import java.io.IOException
import java.util

import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

object MyJedisPool {

  //加载日志对象
  private val logger:Logger = LoggerFactory.getLogger(MyJedisPool.getClass)
  /**
   * 获得jedis对象
   */
  def getObject: JedisCluster = {
    var pool:JedisCluster = null
    try {
      val hosts = StringUtils.split(PropertiesUtil.getProperty("redis.host"), ",")
      val ports = StringUtils.split(PropertiesUtil.getProperty("redis.port"), ",")
      val nodes = new util.HashSet[HostAndPort]
      for (i <- 0 until hosts.length) {
        nodes.add(new HostAndPort(hosts(i), ports(i).toInt))
      }
      //创建jedis池配置实例
      val config = new JedisPoolConfig
      //设置池配置项值
      config.setMaxTotal(PropertiesUtil.getProperty("maxTotal").toInt)
      config.setMaxIdle(PropertiesUtil.getProperty("maxIdle").toInt)
      config.setMaxWaitMillis(PropertiesUtil.getProperty("maxWait").toLong)
      //对拿到的connection进行validateObject校验
      config.setTestOnBorrow(true)
      //根据配置实例化jedis池
      pool = new JedisCluster(nodes, 15000, 15000, 1500, PropertiesUtil.getProperty("redis.password"), config)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        logger.info("redis连接池异常", e)
      }
    }
    pool
  }

  /**
   * 归还jedis对象
   */
  def returnOjbect(jedis: JedisCluster): Unit = {
    if (jedis != null) {
      try {
        jedis.close()
      }catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }
}
