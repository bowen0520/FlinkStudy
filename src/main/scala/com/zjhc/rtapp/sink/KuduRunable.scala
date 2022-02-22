package com.zjhc.rtapp.sink

import com.zjhc.rtapp.utils.PropertiesUtil

class KuduRunable(jobId: String, tableName: String, tp: String, value: String, status: String)
  extends Runnable{
  override def run(): Unit = {
    PropertiesUtil.insertJobReject(jobId, this.tableName, tp, value, status)
  }
}
