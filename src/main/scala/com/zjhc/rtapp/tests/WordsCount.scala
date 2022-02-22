package com.zjhc.rtapp.tests

import org.apache.flink.api.scala.ExecutionEnvironment
import com.zjhc.rtapp.utils.PropertiesUtil

object WordsCount {
  def main(args: Array[String]): Unit = {

    //val env = ExecutionEnvironment.getExecutionEnvironment

    //val text = env.readTextFile(args(0))

    //text.

    val data = new SeqCharSequence("a")+("b")+("c")

    for(i:Int <- 1 to data.length){
      println(i + " " + data(i))
    }


  }
}
