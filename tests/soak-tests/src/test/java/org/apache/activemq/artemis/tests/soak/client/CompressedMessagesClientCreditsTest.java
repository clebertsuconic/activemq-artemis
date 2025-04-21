/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.soak.client;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.client.impl.ClientConsumerImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompressedMessagesClientCreditsTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   ActiveMQServer server;

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();

      this.server = this.createServer(true, true);
      server.start();
   }

   @Test
   public void testCreditsOnRollbackCompressedToRegularMessages() throws Exception {
      final String queueName = "queue";
      final String originalString = "A really long and repeated string".repeat(20000);
      CountDownLatch latch = new CountDownLatch(1);

      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      connectionFactory.setCompressLargeMessage(true);
      connectionFactory.setCompressionLevel(6);

      Connection connection = connectionFactory.createConnection();
      connection.start();

      ActiveMQSession producerSession = (ActiveMQSession) connection.createSession(Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = producerSession.createProducer(producerSession.createQueue(queueName));

      TextMessage sendMessage = producerSession.createTextMessage();
      sendMessage.setText(originalString);

      ActiveMQSession consumerSession = (ActiveMQSession) connection.createSession(Session.SESSION_TRANSACTED);
      ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) consumerSession.createConsumer(consumerSession.createQueue(queueName));

      Queue queue = server.locateQueue(queueName);

      producer.send(sendMessage);

      //Message should be compressed and under the limit of a largeMessage after compression
      assertTrue(queue.peekFirstMessage().getMessage().getBooleanProperty(Message.HDR_LARGE_COMPRESSED));
      assertTrue(queue.peekFirstMessage().getMessage().getPersistentSize() < connectionFactory.getMinLargeMessageSize());

      TextMessage consumedmessage = (TextMessage) consumer.receive(5000);
      assertNotNull(consumedmessage);

      //ClientConsumerImpl consumerImpl = (consumer.)

      producer.send(sendMessage);
      assertNotNull(consumer.receive(1000));

      producer.close();
      consumer.close();
      producerSession.close();
      consumerSession.close();
      connection.close();

   }

}
