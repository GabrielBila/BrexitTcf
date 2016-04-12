package ucl.irdm.brexit.tcf

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Query, Status, TwitterFactory}

import scala.collection.JavaConversions._

/**
  * Created by Gabriel on 3/11/2016.
  */
object TwitterSearch {

  // search specified query

  private val consumerKey = "ELNqONYPSGtT13huripAO31Yg"
  private val consumerSecret = "8Rc1Jq5ygOrUxevVzWCnPmaWW8y8HL6BKfhTrwPifCDNrloaiT"

  private val accessToken = "968617386-UrYeudqY1bKkjd6XmHRxIy2Tku6M8BZsj1dLib2Y"
  private val accessTokenSecret = "7v0LD6JodSNgKzr4ZMEPtTi2uXzPwTtLweV3XFgInKJL1"

  val twitter = {
    val cb = new ConfigurationBuilder()
      .setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    val tf = new TwitterFactory(cb.build())
    tf.getInstance()
  }

  def buildSearchQuery(terms: Array[String] = Array(),
                       filters: Array[String] = Array("retweets"),
                       lang: String = "en",
                       count: Int = 100,
                       since: String,
                       until: String): Query = {

    val queryArgument = s"${terms.mkString(" OR ")}"

    val query = new Query(queryArgument)
      .count(count)
      .since(since)
      .until(until)
    query
  }

  def getTweets(query: Query): Stream[Status] = {

    def recursiveSearch(query: Query): Stream[Status] = {
      val searchResults = twitter.search(query)
      val currentPageTweets = searchResults.getTweets
      if (searchResults.hasNext) {
        if (searchResults.getRateLimitStatus.getRemaining <= 0) {
          println(s"Request rate limit exceeded; waiting ${searchResults.getRateLimitStatus.getSecondsUntilReset} seconds")
          Thread.sleep(searchResults.getRateLimitStatus.getSecondsUntilReset * 1000 + 5000)
        }
        currentPageTweets.toStream #::: recursiveSearch(searchResults.nextQuery())
      }
      else {
        currentPageTweets.toStream
      }
    }

    recursiveSearch(query)
  }

}

import ucl.irdm.brexit.tcf.TwitterSearch._

object Main {

  val dateFileFormat = new SimpleDateFormat("yyyy-MM-dd")

  val startTime = OffsetDateTime.parse("2016-03-09T00:00:00+00:00", DateTimeFormatter.ISO_DATE_TIME)
  val endTime = OffsetDateTime.parse("2016-03-11T00:00:00+00:00", DateTimeFormatter.ISO_DATE_TIME)

  val terms = Array("#brexit", "#no2eu", "#notoeu", "#betteroffout", "#voteout", "#britainout",
    "#leaveeu", "#loveeuropeleaveeu", "#voteleave", "#beleave", "#yes2eu", "#yestoeu",
    "#betteroffin", "#votein", "#ukineu", "#bremain", "#strongerin", "#leadnotleave", "#voteremain")

  def main(args: Array[String]): Unit = {
    val dailyTweets = daysBetween(startTime, endTime) map (day => {
      val start = dateString(day)
      val end = dateString(day plusDays 1)
      val query = buildSearchQuery(terms = terms, since = start, until = end)
      getTweets(query)
    })

    dailyTweets foreach (dayTweets => {
      val filename = s"${dateFileFormat.format(dayTweets.head.getCreatedAt)}.json"
      println(s"Printing tweets for date: $filename")
      printTweetsToFile(filename, dayTweets)
    })
  }

  def daysBetween(start: OffsetDateTime, end: OffsetDateTime): Stream[OffsetDateTime] =
    if (start isBefore end)
      start #:: daysBetween(start plusDays 1, end)
    else
      Stream.empty

  def dateString(date: OffsetDateTime) = {
    val year = date.getYear
    val month = "%02d" format date.getMonthValue
    val day = "%02d" format date.getDayOfMonth
    s"$year-$month-$day"
  }

  def printTweetsToFile(filename: String, tweets: Stream[Status]): Unit = {
    val outputFile = new File(filename)
    outputFile.createNewFile()
    val out = new FileWriter(outputFile)

    val gen = new JsonFactory().createGenerator(out)
    gen.useDefaultPrettyPrinter()
    gen.setCodec(new ObjectMapper().registerModule(DefaultScalaModule))

    gen.writeStartArray()
    tweets foreach (gen writeObject _)
    gen.writeEndArray()

    gen.close()
    out.close()
  }

}