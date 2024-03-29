package com.challenge.test

import org.apache.spark._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
  * Created by saleh on 10/22/17.
  */


trait LocalSparkContext extends BeforeAndAfterEach
  with BeforeAndAfterAll {
  self: Suite =>

  @transient var sc: SparkContext = _


  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() {
    LocalSparkContext.stop(sc)
    sc = null
  }

}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    Option(sc).foreach { ctx =>
      ctx.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't
    // unbind immediately on shutdown.
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }
}