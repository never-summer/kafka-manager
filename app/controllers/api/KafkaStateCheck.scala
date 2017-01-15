/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package controllers.api

import java.util.Properties

import controllers.KafkaManagerContext
import features.ApplicationFeatures
import models.navigation.Menus
import org.apache.kafka.clients.consumer.KafkaConsumer
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.Future
import org.json4s.jackson.Serialization
import org.json4s.scalaz.JsonScalaz.toJSON
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._

/**
 * @author jisookim0513
 */

class KafkaStateCheck (val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
                      (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def brokers(c: String) = Action.async { implicit request =>
    kafkaManager.getBrokerList(c).map { errorOrBrokerList =>
      errorOrBrokerList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        brokerList => Ok(Json.obj("brokers" -> brokerList.list.map(bi => bi.id).sorted))
      )
    }
  }

  def topics(c: String) = Action.async { implicit request =>
    kafkaManager.getTopicList(c).map { errorOrTopicList =>
      errorOrTopicList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicList => Ok(Json.obj("topics" -> topicList.list.sorted))
      )
    }
  }

  def topicIdentities(c: String) = Action.async { implicit request =>
    implicit val formats = org.json4s.DefaultFormats
    kafkaManager.getTopicListExtended(c).map { errorOrTopicListExtended =>
      errorOrTopicListExtended.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicListExtended => Ok(Serialization.writePretty("topicIdentities" -> topicListExtended.list.flatMap(_._2).map(toJSON(_))))
      )
    }
  }

  def clusters = Action.async { implicit request =>
    implicit val formats = org.json4s.DefaultFormats
    kafkaManager.getClusterList.map { errorOrClusterList =>
      errorOrClusterList.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        clusterList => Ok(Serialization.writePretty("clusters" -> errorOrClusterList.toOption))
      )
    }
  }

  def underReplicatedPartitions(c: String, t: String) = Action.async { implicit request =>
    kafkaManager.getTopicIdentity(c, t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicIdentity => Ok(Json.obj("topic" -> t, "underReplicatedPartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isUnderReplicated).map{case (num, pi) => pi.partNum}))
      )
    }
  }

  def unavailablePartitions(c: String, t: String) = Action.async { implicit request =>
    kafkaManager.getTopicIdentity(c, t).map { errorOrTopicIdentity =>
      errorOrTopicIdentity.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicIdentity => Ok(Json.obj("topic" -> t, "unavailablePartitions" -> topicIdentity.partitionsIdentity.filter(_._2.isr.isEmpty).map { case (num, pi) => pi.partNum })))
    }
  }

  def topicSummaryAction(cluster: String, consumer: String, topic: String, consumerType: String) = Action.async { implicit request =>
    getTopicSummary(cluster, consumer, topic, consumerType).map { errorOrTopicSummary =>
      errorOrTopicSummary.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        topicSummary => {
          Ok(topicSummary)
        })
    }
  }

  def getTopicSummary(cluster: String, consumer: String, topic: String, consumerType: String) = {
    kafkaManager.getConsumedTopicState(cluster, consumer, topic, consumerType).map { errorOrTopicSummary =>
      errorOrTopicSummary.map(
        topicSummary => {
          Json.obj("totalLag" -> topicSummary.totalLag, "percentageCovered" -> topicSummary.percentageCovered, "partitionOffsets" -> topicSummary.partitionOffsets.map {case (pnum, offset) => offset}, "partitionLatestOffsets" -> topicSummary.partitionLatestOffsets.map {case (pnum, latestOffset) => latestOffset}, "owners" -> topicSummary.partitionOwners.map {case (pnum, owner) => owner}   )
        })
    }
  }

  def groupSummaryAction(cluster: String, consumer: String, consumerType: String) = Action.async { implicit request =>
    kafkaManager.getConsumerIdentity(cluster, consumer, consumerType).flatMap { errorOrConsumedTopicSummary =>
      errorOrConsumedTopicSummary.fold(
        error =>
          Future.successful(BadRequest(Json.obj("msg" -> error.msg))),
        consumedTopicSummary => getGroupSummary(cluster, consumer, consumedTopicSummary.topicMap.keys, consumerType).map { topics =>
          Ok(JsObject(topics))
        })
    }
  }

  def getGroupSummary(cluster: String, consumer: String, groups: Iterable[String], consumerType: String): Future[Map[String, JsObject]] = {
    val cosumdTopicSummary: List[Future[(String, JsObject)]] = groups.toList.map { group =>
      getTopicSummary(cluster, consumer, group, consumerType)
        .map(topicSummary => group -> topicSummary.getOrElse(Json.obj()))
    }
    Future.sequence(cosumdTopicSummary).map(_.toMap)
  }
  
  def consumersSummaryAction(cluster: String) = Action.async { implicit request =>
    implicit val formats = org.json4s.DefaultFormats
    kafkaManager.getConsumerListExtended(cluster).map { errorOrConsumersSummary =>
      errorOrConsumersSummary.fold(
        error => BadRequest(Json.obj("msg" -> error.msg)),
        consumersSummary => Ok(Serialization.writePretty("consumers" -> consumersSummary.list.map{case ((consumer, consumerType), consumerIdentity) => Map("name" -> consumer, "type" -> consumerType.toString, "topics" -> consumerIdentity.map(_.topicMap.keys))}))
        )
    }
  }
  def echo(topic: String) = Action { request =>
      val consumer = {
        val props = getProperties(servers = "localhost:9092", groupId = "test02")
        new KafkaConsumer[String, String](props)
      }
      // Subscribe to one topic
      consumer.subscribe(List(topic))
      consumer.poll(0)
      var sumOffset = 0l
      consumer.assignment().foreach { tp =>
        consumer.seekToEnd(tp)
        sumOffset += consumer.position(tp)
      }
      consumer.close()
      Ok("sumOffset:" + sumOffset)
  }

  def getProperties(servers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", servers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }
}
