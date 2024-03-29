package com.challenge.test

import com.challenge.Utils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by saleh on 10/22/17.
  */


trait SparkContextProvider {
  def sc: SparkContext

  def appID: String = (this.getClass.getName
    + math.floor(math.random * 10E4).toLong.toString)

  def conf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost")
  }


  /**
    * Setup work to be called when creating a new SparkContext. Default implementation
    * currently sets a checkpoint directory.
    *
    * This _should_ be called by the context provider automatically.
    */
  def setup(sc: SparkContext): Unit = {
    sc.setCheckpointDir(Utils.createTempDir().toPath().toString)
  }
}