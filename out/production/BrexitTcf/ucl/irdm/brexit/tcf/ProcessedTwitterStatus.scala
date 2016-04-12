package ucl.irdm.brexit.tcf

import java.util.Date

import twitter4j.{GeoLocation, HashtagEntity, Status}

/**
  * Created by Gabriel on 3/14/2016.
  */
case class ProcessedTwitterStatus(id: Long,
                                  text: String,
                                  dateCreated: Date,
                                  location: GeoLocation,
                                  userId: Long,
                                  hashtags: Array[HashtagEntity])

object ProcessedTwitterStatus {
  def apply(tweet: Status) =
    new ProcessedTwitterStatus(id = tweet.getId,
      text = tweet.getText,
      dateCreated = tweet.getCreatedAt,
      location = tweet.getGeoLocation,
      userId = tweet.getUser.getId,
      hashtags = tweet.getHashtagEntities
    )
}