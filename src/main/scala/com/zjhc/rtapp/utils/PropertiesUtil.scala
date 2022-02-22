package com.zjhc.rtapp.utils

import java.io.FileInputStream
import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import com.mysql.jdbc.Connection

object PropertiesUtil {
  private val prop = new Properties()
  //获取配置文件路径
  private val path = Thread.currentThread().getContextClassLoader.getResource("test.properties").getPath
  //加载配置文件的内容
  prop.load(new FileInputStream(path))

  /**
   * 通过传入的key获取配置文件里的value值
   * @param key
   * @return
   */
  def getProperty(key:String): String ={
    prop.getProperty(key)
  }

  /**
   * 获取数据库连接对象
   * @return
   */
  def getDbConnection(): Connection = {
    try {
      //加载驱动
      Class.forName(getProperty("database.connection.driver"))
      //获取连接
      val conn = DriverManager.getConnection(getProperty("database.connection.url")
        , getProperty("database.connection.user")
        , getProperty("database.connection.password"))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    null
  }

  /**
   * 向JOB_REJECTED表中添加数据
   * @param data :向表中添加的数据
   */
  def insertJobReject(data:String*): Unit = {
    try {
      val conn = getDbConnection()
      val pst = conn.prepareStatement("insert into JOB_REJECTED (JOB_ID,TABLE_NAME,REJECTED_TYPE,REJECTED_DATA,REJECTED_REASON,REJECTED_TIME) values(?,?,?,?,?,sysdate())")
      for (i:Int <- 0 until data.length) {
        pst.setString(i+1, data(i))
      }
      pst.executeUpdate()
      //println("执行插入语句成功");
      pst.close()
      conn.close()
    } catch {
      //println("执行插入语句失败")
      case e:Exception => e.printStackTrace()
    }
  }

  /**
   * 执行DQL语句获取返回对象ResultSet，通过ResultSet获取结果集
   * @param sql
   * @return
   */
  def getData(sql:String):ResultSet = {
    try {
      val conn = getDbConnection()
      val pst = conn.prepareStatement(sql)
      val resultSet = pst.executeQuery()
    } catch {
      //println("执行查询语句失败")
      case e:Exception => e.printStackTrace()
    }
    null
  }

  /**
   * 通过sql语句查询数据，并将数据返回
   * @param sql
   * @param targetName
   * @return
   */
  def getDataList(sql:String, targetName:String) : String ={
    var result:String = null
    //List<>
    try {
      val conn = getDbConnection()
      val pst = conn.prepareStatement(sql)
      val resultSet = pst.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getString(targetName)
      }
    } catch {
      //println("执行查询语句失败");
      case e:Exception => e.printStackTrace()
    }
    result
  }
}
