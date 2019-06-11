package edp.rider.kafka

import edp.rider.common.KafkaVersion


object WormholeGetOffsetUtils {
  def getLatestOffset(brokerList: String, brokerVersion: String, topic: String, kerberos: Boolean = false): String = {
    adaptKafkaVersion(brokerVersion) match {
        case KafkaVersion.KAFKA_010 => edp.wormhole.kafka010.WormholeGetOffsetUtils.getLatestOffset(brokerList,topic,kerberos)
        case KafkaVersion.KAFKA_0102=> edp.wormhole.kafka.WormholeGetOffsetUtils.getLatestOffset(brokerList,topic,kerberos)
      }
  }

  def getEarliestOffset(brokerList: String,brokerVersion: String, topic: String, kerberos: Boolean = false): String = {
    adaptKafkaVersion(brokerVersion) match {
      case KafkaVersion.KAFKA_010 => edp.wormhole.kafka010.WormholeGetOffsetUtils.getEarliestOffset(brokerList,topic,kerberos)
      case KafkaVersion.KAFKA_0102 => edp.wormhole.kafka.WormholeGetOffsetUtils.getEarliestOffset(brokerList,topic,kerberos)
    }
  }

  def getConsumerOffset(brokers: String,brokerVersion: String, groupId: String, topic: String, partitions: Int, kerberos: Boolean): String = {
    adaptKafkaVersion(brokerVersion) match {
      case KafkaVersion.KAFKA_010 => edp.wormhole.kafka010.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
      case KafkaVersion.KAFKA_0102 => edp.wormhole.kafka.WormholeGetOffsetUtils.getConsumerOffset(brokers,groupId,topic,partitions,kerberos)
    }
  }

  private def adaptKafkaVersion(version: String)={
    val versions=version.split("\\.")
    if(versions(0).toInt > 0 || versions(1).toInt > 10 || versions(2).toInt >= 2)
      KafkaVersion.KAFKA_0102
    else
      KafkaVersion.KAFKA_010
  }
}
