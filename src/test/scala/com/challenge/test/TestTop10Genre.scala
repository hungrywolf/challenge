package com.challenge.test

import org.apache.spark.sql._
import org.scalatest.FunSuite

/**
  * Created by saleh on 10/22/17.
  */

case class Anime(anime_id: Int, name: String, genre: String, Type: String, episodes: String, rating: Double, members: Long)
case class AnimeCount(anime_id: Int, name: String, genre: String, Type: String, episodes: String, rating: Double, members: Long, count:Long)
case class Rating(user_id:Int, anime_id:Int, rating:Double)
case class RatingCount(anime_id:Int, count:Long)
case class Genre(genre:String , counts:Long)

class TestTop10Genre extends FunSuite with PerSparkContext {
  val DATAPRICK = "com.databricks.spark.csv"
  val ANIME_FILE = "src/test/resources/data/anime.csv"
  val RATING_FILE = "src/test/resources/data/rating.csv.gz"
  val HEADER = "header"
  val TRUE = "true"
  val INFO = "inferSchema"

  beforeAll()
  test("Top 10 Genre") {

    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    val animeEncoder = Seq(Anime(0, "", "", "", "", 0.0, 0)).toDS
    val animeCountEncoder = Seq(AnimeCount(0, "", "", "", "", 0.0, 0, 0)).toDS
    val ratingEncoder = Seq(Rating(0, 0, 0.0)).toDS
    val ratingCountEncoder = Seq(RatingCount(0, 0)).toDS
    val genreEncoder = Seq(Genre("", 0)).toDS

    // load anime csv
    val anime: Dataset[Anime] = sqlContext.read.format(DATAPRICK).option(HEADER, TRUE)
      .option(INFO, TRUE).load(ANIME_FILE).as[Anime]

    // load rating csv
    val rating: Dataset[Rating] = sqlContext.read.format(DATAPRICK).option(HEADER, TRUE)
      .option(INFO, TRUE).load(RATING_FILE).as[Rating]

    // filtering data get only TV and ep mpre that 10 and rated anime
    val filterAnime = anime
      .select("anime_id", "name", "genre", "Type", "episodes", "rating", "members")
      .where("episodes >= 10").where("Type='TV'").where("rating != -1").as[Anime]

    // get rating count for each anime
    val rating_count: Dataset[RatingCount] = rating.groupBy("anime_id").count().as[RatingCount]

    // join filtered anime table with counts
    val anime_count: Dataset[AnimeCount] = filterAnime
      .join(rating_count,filterAnime.col("anime_id") === rating_count.col("anime_id"),"inner")
      .drop(rating_count.col("anime_id")).as[AnimeCount]

    // explode table by genre using the genre array
    val explodAnime: Dataset[AnimeCount] = anime_count
      .flatMap(a => a.genre.split(",")
        .map(g => AnimeCount(a.anime_id,a.name, g.trim,a.Type,a.episodes,a.rating,a.members,a.count)))
      .as[AnimeCount]

    // sum the counts base on the genre
    val finalResult: Dataset[Genre] = explodAnime.groupBy("genre").sum("count")
      .withColumnRenamed("sum(count)","counts").sort($"counts".desc).limit(10).as[Genre]


    assert(finalResult.collect().length == 10)
    finalResult.show(10)

  }

  afterAll()

}
