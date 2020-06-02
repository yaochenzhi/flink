package com.yaochenzhi.flink

import java.io.File
import org.apache.flink.api.scala._
/*
    IntelliJ IDEA scala env set up:
      . right click project directory in idea and the click add framework support.
          . if scala is not in the options, go to 'File' -> 'Settings' -> 'Plugins' and install scala
      . right click the src directory in idea and mark directory as resources root and then
          add scala class object as we need.
      .

 */


// batch mode processing from static file
object WordCount {

  def main(args: Array[String]): Unit = {

    // create env
    var env = ExecutionEnvironment.getExecutionEnvironment

    var inputDataSet = env.readTextFile(getInputFilePath)

    /*
    var wordCountDataSet = inputDataSet.flatMap( line => line.split(" "))*/
    var wordCountDataSet = inputDataSet.flatMap( _.split(" "))
      .map( (_, 1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()

  }

  def getInputFilePath: String = {
    var projectPath = new File(WordCount.getClass.getClassLoader.getResource("").getPath).getParent
    projectPath = new File(projectPath).getParent
    new File(projectPath, "src/main/resources/test.txt").getPath
  }

}

