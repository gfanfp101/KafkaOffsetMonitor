package com.quantifind.kafka

import scala.collection._

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, KafkaInfo, OffsetInfo}
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{KafkaException, BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, Logging, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import com.twitter.util.Time
import org.apache.zookeeper.data.Stat
import scala.util.control.NonFatal
import scala.util.parsing.json.JSONObject
import scala.collection.immutable.ListMap

/**
 * a nicer version of kafka's ConsumerOffsetChecker tool
 * User: pierre
 * Date: 1/22/14
 */

case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])

case class ConsumerDetail(name: String)

class OffsetGetter(zkClient: ZkClient) extends Logging {

  private val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

  private def getConsumer(bid: Int): Option[SimpleConsumer] = {
    try {
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + bid) match {
        case (Some(brokerInfoString), _) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case (None, _) =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        error("Could not parse broker info", t)
        None
    }
  }

  private def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {
    try {
      val (partitionInfoJson, stat: Stat) = ZkUtils.readData(zkClient, s"/kafkastorm2/$group/partition_$pid")
      val partitionInfo = Json.parseFull(partitionInfoJson).getOrElse(throw new KafkaException("Partition id %pid does not exist".format(pid))).asInstanceOf[Map[String, Any]]
      val offset = partitionInfo.get("offset").get.asInstanceOf[Int].asInstanceOf[Long]

      ZkUtils.getLeaderForPartition(zkClient, topic, pid) match {
        case Some(bid) =>
          val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt map {
            consumer =>
              val topicAndPartition = TopicAndPartition(topic, pid)
              val requestLogSize =
                OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logSize = consumer.getOffsetsBefore(requestLogSize).partitionErrorAndOffsets(topicAndPartition).offsets.head

              val requestStartPoint =
                OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
              val startPoint = consumer.getOffsetsBefore(requestStartPoint).partitionErrorAndOffsets(topicAndPartition).offsets.head

              OffsetInfo(group = group,
                topic = topic,
                partition = pid,
                offset = offset,
                logSize = logSize,
                startPoint = Some(startPoint),
                owner = Option(group),
                creation = Time.fromMilliseconds(stat.getCtime),
                modified = Time.fromMilliseconds(stat.getMtime))
          }
        case None =>
          error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case NonFatal(t) =>
        error(s"Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  private def processTopic(group: String, topic: String): Seq[OffsetInfo] = {
    val pidMap = ZkUtils.getPartitionsForTopics(zkClient, Seq(topic))
    for {
      partitions <- pidMap.get(topic).toSeq
      pid <- partitions.sorted
      info <- processPartition(group, topic, pid)
    } yield info
  }

  private def brokerInfo(): Iterable[BrokerInfo] = {
    for {
      (bid, consumerOpt) <- consumerMap
      consumer <- consumerOpt
    } yield BrokerInfo(id = bid, host = consumer.host, port = consumer.port)
  }

  def offsetInfo(group: String, topics: Seq[String] = Seq()): Seq[OffsetInfo] = {

    val topicList = if (topics.isEmpty) {
      try {
        ZkUtils.getChildren(
          zkClient, s"${ZkUtils.BrokerTopicsPath}").toSeq
      } catch {
        case _: ZkNoNodeException => Seq()
      }
    } else {
      topics
    }
    topicList.sorted.flatMap(processTopic(group, _))
  }

  def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo = {
    val off = offsetInfo(group, topics)
    val brok = brokerInfo()
    KafkaInfo(
      brokers = brok.toSeq,
      offsets = off
    )
  }

  def singleOffRatio(group: String, topic: String): JSONObject = {
    val offsets = offsetInfo(group, Seq(topic))
    val offsetSum = offsets.view.map(_.offset).sum
    val logSizeSum = offsets.view.map(_.logSize).sum
    val startPointSum = offsets.view.map(_.startPoint.getOrElse(0L)).sum
    val lagSum = offsets.view.map(_.lag).sum
    val result = ListMap[String, Any](
      "group" -> group,
      "topic" -> topic,
      "offsetSum" -> offsetSum,
      "logSizeSum" -> logSizeSum,
      "startPointSum" -> startPointSum,
      "lagSum" -> lagSum,
      "ratio (abt)" -> lagSum.toDouble/(logSizeSum-startPointSum),
      "ratio (%)" -> (lagSum.toDouble/(logSizeSum-startPointSum)*100).toInt)
    JSONObject(result toMap)
  }

  def getGroups: Seq[String] = {
    try {
      ZkUtils.getChildren(zkClient, "/kafkastorm2")
    } catch {
      case NonFatal(t) =>
        error(s"could not get groups because of ${t.getMessage}", t)
        Seq()
    }
  }


  /**
   * returns details for a given topic such as the active consumers pulling off of it
   * @param topic
   * @return
   */
  def getTopicDetail(topic: String): TopicDetails = {
    val topicMap = getActiveTopicMap

    if (topicMap.contains(topic)) {
      TopicDetails(topicMap(topic).map(consumer => {
        ConsumerDetail(consumer.toString)
      }).toSeq)
    } else {
      TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
    }
  }

  def getTopics: Seq[String] = {
    try {
      ZkUtils.getChildren(zkClient, ZkUtils.BrokerTopicsPath).sortWith(_ < _)
    } catch {
      case NonFatal(t) =>
        error(s"could not get topics because of ${t.getMessage}", t)
        Seq()

    }
  }


  /**
   * returns a map of active topics-> list of consumers from zookeeper, ones that have IDS attached to them
   *
   * @return
   */
  def getActiveTopicMap: Map[String, Seq[String]] = {
    ZkUtils.getChildren(zkClient, "/kafkastorm2").map {
      group =>
        ZkUtils.getChildren(zkClient, s"/kafkastorm2/$group").map {
          pid => 
            val (partitionInfoJson, _) = ZkUtils.readData(zkClient, s"/kafkastorm2/$group/$pid")
            val partitionInfo = Json.parseFull(partitionInfoJson).getOrElse(throw new KafkaException("Partition id %pid does not exist".format(pid))).asInstanceOf[Map[String, Any]]
            val topic = partitionInfo.get("topic").get.asInstanceOf[String]
            Tuple2(topic, group)
        }
      }.foldLeft(Seq.empty[(String, String)])(_++_).groupBy(_._1).mapValues {
        _.unzip._2.toList.distinct
      }
  }

  def getActiveTopics: Node = {
    val topicMap = getActiveTopicMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  def getClusterViz: Node = {
    val clusterNodes = ZkUtils.getAllBrokersInCluster(zkClient).map((broker) => {
      Node(broker.getConnectionString(), Seq())
    })
    Node("KafkaCluster", clusterNodes)
  }

  def close() {
    for (consumerOpt <- consumerMap.values) {
      consumerOpt match {
        case Some(consumer) => consumer.close()
        case None => // ignore
      }
    }
  }

}

object OffsetGetter {

  case class KafkaInfo(brokers: Seq[BrokerInfo], offsets: Seq[OffsetInfo])

  case class BrokerInfo(id: Int, host: String, port: Int)

  case class OffsetInfo(group: String,
                        topic: String,
                        partition: Int,
                        offset: Long,
                        logSize: Long,
                        startPoint: Option[Long],
                        owner: Option[String],
                        creation: Time,
                        modified: Time) {
    val lag = logSize - offset
  }

}