package ucl.irdm.brexit.tcf

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.Date

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.Logger
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Query, Status, TwitterFactory}

import scala.collection.JavaConversions._

/**
  * Created by Gabriel on 3/11/2016.
  */
object TwitterSearch {

  val twitterDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  private val logger = Logger.getLogger(TwitterSearch.getClass)

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

    logger.info(s"Building query for day $since")

    val queryArgument = s"${terms.mkString(" OR ")}"

    val query = new Query(queryArgument)
      .count(count)
      .since(since)
      .until(until)
    query
  }

  def getTweets(query: Query): (Date, Stream[Status]) = {

    val date = twitterDateFormat.parse(query.getSince)

    def recursiveSearch(query: Query): Stream[Status] = {
      val searchResults = twitter.search(query)
      val currentPageTweets = searchResults.getTweets

      if (searchResults.hasNext) {
        if (searchResults.getRateLimitStatus.getRemaining <= 0) {
          logger.info(s"Request rate limit exceeded; waiting ${searchResults.getRateLimitStatus.getSecondsUntilReset} seconds")
          Thread.sleep(searchResults.getRateLimitStatus.getSecondsUntilReset * 1000 + 5000)
        }
        currentPageTweets.toStream #::: recursiveSearch(searchResults.nextQuery())
      }
      else {
        currentPageTweets.toStream
      }
    }

    (date, recursiveSearch(query))
  }

}

import ucl.irdm.brexit.tcf.TwitterSearch._

object Main {

  val logger = Logger.getLogger(Main.getClass)

//  val startTime = OffsetDateTime.parse("2016-04-02T00:00:00+00:00", DateTimeFormatter.ISO_DATE_TIME)
//  val endTime = OffsetDateTime.parse("2016-04-13T00:00:00+00:00", DateTimeFormatter.ISO_DATE_TIME)

  val terms = Array("#brexit", "#no2eu", "#notoeu", "#betteroffout", "#voteout", "#britainout",
    "#leaveeu", "#loveeuropeleaveeu", "#voteleave", "#beleave", "#yes2eu", "#yestoeu",
    "#betteroffin", "#votein", "#ukineu", "#bremain", "#strongerin", "#leadnotleave", "#voteremain")

  def processArgs(args: Array[String]): Option[(OffsetDateTime, OffsetDateTime)] = {
    try {
      val startTime = OffsetDateTime.parse(s"${args(0)}T00:00:00+00:00", DateTimeFormatter.ISO_DATE_TIME)
      val endTime = OffsetDateTime.parse(s"${args(1)}T00:00:00+00:00", DateTimeFormatter.ISO_DATE_TIME)
      Some(startTime, endTime)
    } catch {
      case _: Throwable => None
    }
  }

  val usage: String = "Usage: BrexitTCF <start_date> <end_date>\n" +
    "<start_date> the first day for which to get the Brexit tweets.\n" +
    "<end_date> the exclusive end of the date range. Will only return tweets up to the day before <end_date>.\n" +
    "<start_date> and <end_date> must follow format: yyyy-MM-dd"

  def main(args: Array[String]): Unit = {
    val argDates = processArgs(args)
    if (argDates.isEmpty) {
      println(usage)
      return
    }
    val (startTime, endTime) = argDates.get

    val dailyTweets = daysBetween(startTime, endTime) map (day => {
      val start = dateString(day)
      val end = dateString(day plusDays 1)
      val query = buildSearchQuery(terms = terms, since = start, until = end)
      getTweets(query)
    })

    dailyTweets foreach {
      case (d, Stream.Empty) =>
        logger.info(s"No tweets found")
      case (d, dayTweets) =>
        val filename = s"${twitterDateFormat.format(d)}.json"
        logger.info(s"Printing tweets for date: $filename")
        printTweetsToFile(filename, dayTweets)
    }
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