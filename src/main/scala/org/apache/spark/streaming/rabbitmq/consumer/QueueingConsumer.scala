/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.rabbitmq.consumer

import com.rabbitmq.client.{AMQP, Channel, DefaultConsumer, Envelope}
import com.rabbitmq.client.Delivery
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

private[rabbitmq]
class QueueingConsumer(channel: Channel) extends DefaultConsumer(channel) {

  val _queue = new LinkedBlockingQueue[Delivery]()

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
    _queue.add(new Delivery(envelope, properties, body))
  }

  def nextDelivery(): Delivery = {
    _queue.take()
  }

  def nextDelivery(timeout: Long): Delivery = {
    _queue.poll(timeout, TimeUnit.MILLISECONDS)
  }
}