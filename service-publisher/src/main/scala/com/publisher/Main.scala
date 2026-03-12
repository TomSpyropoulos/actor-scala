package com.publisher

import org.apache.pekko.stream.connectors.mqtt.MqttConnectionSettings

object Main {
  def main(args: Array[String]): Unit = {
    val connectionSettings = MqttConnectionSettings()
  }
}
