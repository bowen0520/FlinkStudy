package com.zjhc.rtapp.utils

import com.google.common.collect.ImmutableBiMap
import org.apache.kudu.Type
import org.apache.kudu.client.PartialRow
import org.apache.kudu.client.RowResult
import java.nio.ByteBuffer
import java.util.Date

object TableUtil {
  //将类型作为key，数据类型的字节码对象作为value
  private val TYPES = new ImmutableBiMap.Builder[Type, Class[_]]
      .put(Type.STRING, classOf[String])
      .put(Type.BOOL, classOf[Boolean])
      .put(Type.DOUBLE, classOf[Double])
      .put(Type.FLOAT, classOf[Float])
      .put(Type.BINARY, classOf[ByteBuffer])
      .put(Type.INT8, classOf[Byte])
      .put(Type.INT16, classOf[Short])
      .put(Type.INT32, classOf[Integer])
      .put(Type.INT64, classOf[Long])
      .put(Type.UNIXTIME_MICROS, classOf[Date]).build

  /**
   * 通过字节码对象获取数据类型
   * @param clazz ：数据类型得字节码文件
   * @return
   */
  def getType(clazz: Class[_]): Type = TYPES.inverse.get(clazz)

  /**
   * 通过数据类型获取字节码对象
   * @param tp
   * @return
   */
  def getClass(tp: Type): Class[_] = TYPES.get(tp)

  /**
   * 获取参数row的col字段名获取该字段的数据
   * @param row
   * @param col
   * @return
   */
  def valueFromRow(row: RowResult, col: String): Any = {
    var value = new Any
    val colType = row.getColumnType(col)
    colType match {
      case Type.STRING => value = row.getString(col)
      case Type.BOOL => value = row.getBoolean(col)
      case Type.DOUBLE => value = row.getDouble(col)
      case Type.FLOAT => value = row.getFloat(col)
      case Type.BINARY => value = row.getBinary(col)
      case Type.INT8 => value = row.getByte(col)
      case Type.INT16 => value = row.getShort(col)
      case Type.INT32 => value = row.getInt(col)
      case Type.INT64 => value = row.getLong(col)
      case Type.UNIXTIME_MICROS => value = new Date(row.getLong(col))
      //value = new Date(row.getLong(col)/1000)
    }
    value
  }

  /**
   * 将value转换为clazz字节码对象对应得对象
   * @param value
   * @param clazz
   * @tparam T
   * @return
   */
  private def mapValue[T](value: Any, clazz: Class[_]) = value.asInstanceOf[T]

  /**
   * 将value转换成colType类型作为值
   * 以col为键，存入row内
   * @param row
   * @param colType
   * @param col
   * @param value
   */
  def valueToRow(row: PartialRow, colType: Type, col: String, value: Any): Unit = {
    colType match {
      case Type.STRING => row.addString(col, mapValue(value, getClass(colType)))
      case Type.BOOL => row.addBoolean(col, mapValue(value, getClass(colType)))
      case Type.DOUBLE => row.addDouble(col, mapValue(value, getClass(colType)))
      case Type.FLOAT => row.addFloat(col, mapValue(value, getClass(colType)))
      case Type.BINARY => //row.addBinary(col, mapValue(value, mapFromType(colType)));
      case Type.INT8 => row.addByte(col, mapValue(value, getClass(colType)))
      case Type.INT16 => row.addShort(col, mapValue(value, getClass(colType)))
      case Type.INT32 => row.addInt(col, mapValue(value, getClass(colType)))
      case Type.INT64 => row.addLong(col, mapValue(value, getClass(colType)))
      case Type.UNIXTIME_MICROS => row.addLong(col, mapValue(value, getClass(colType)))
    }
  }
}
