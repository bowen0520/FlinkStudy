package com.zjhc.rtapp.utils

import redis.clients.jedis.JedisCluster

object RedisUtil {
  /**
   * 将传入的键值对写入
   * @param k
   * @param v
   */
  def set(k: String, v: String): String = {
    var jedis:JedisCluster = null
    try {
      jedis = MyJedisPool.getObject
      //添加键值对数据
      jedis.set(k, v)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MyJedisPool.returnOjbect(jedis)
    }
    null
  }

  /**
   * 与键对应的值加一 "2" >> "3"
   * @param k
   */
  def incr(k: String): Long = {
    var jedis:JedisCluster = null
    try {
      jedis = MyJedisPool.getObject
      jedis.incr(k)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MyJedisPool.returnOjbect(jedis)
    }
    null
  }

  /**
   * 根据key取value
   *
   * @param k
   * @return
   */
  def get(k: String): String = {
    var jedis:JedisCluster = null
    try {
      jedis = MyJedisPool.getObject
      jedis.get(k)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MyJedisPool.returnOjbect(jedis)
    }
    null
  }
}
