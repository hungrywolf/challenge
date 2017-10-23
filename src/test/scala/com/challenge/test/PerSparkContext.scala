package com.challenge.test

import org.apache.spark._
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
  * Created by saleh on 10/22/17.
  */

trait PerSparkContext extends LocalSparkContext with BeforeAndAfterEach
  with SparkContextProvider {
  self: Suite =>

  override def beforeEach() {
    sc = new SparkContext(conf)
    setup(sc)
    super.beforeEach()
  }

  override def afterEach() {
    super.afterEach()
  }
}