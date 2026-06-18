/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.cluster.mirror;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameter;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test verifies that large messages sent via different protocols (AMQP, CORE, OPENWIRE)
 * are correctly mirrored to a second node when they expire and are diverted to another queue.
 *
 * Test flow:
 * 1. Send large messages to a source queue with expiry time
 * 2. Messages expire and are moved to an expiry queue
 * 3. A divert on the expiry queue forwards messages to a diverted queue
 * 4. Verify all messages are present on the second node (mirrored)
 */
@ExtendWith(ParameterizedTestExtension.class)
public class LargeMessageExpiryDivertMirrorTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String SOURCE_QUEUE = "SourceQueue";
   private static final String EXPIRY_QUEUE = "ExpiryQueue";
   private static final String DIVERTED_QUEUE = "DivertedQueue";

   private static final int MESSAGE_SIZE = 300 * 1024; // 300KB to ensure large message
   private static final int NUMBER_OF_MESSAGES = 10;

   @Parameters(name = "protocol={0}")
   public static Iterable<Object[]> data() {
      return java.util.Arrays.asList(new Object[][] {
         {"AMQP"},
         {"CORE"},
         {"OPENWIRE"}
      });
   }

   @Parameter(index = 0)
   public String protocol;

   private ActiveMQServer server1;
   private ActiveMQServer server2;

   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();

      // Configure server 1
      Configuration config1 = createDefaultNettyConfig()
         .setName("server1")
         .setBindingsDirectory(getBindingsDir(0, false))
         .setJournalDirectory(getJournalDir(0, false))
         .setPagingDirectory(getPageDir(0, false))
         .setLargeMessagesDirectory(getLargeMessagesDir(0, false))
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration("netty", "tcp://localhost:61616");

      config1.addQueueConfiguration(QueueConfiguration.of(SOURCE_QUEUE).setRoutingType(RoutingType.ANYCAST));
      config1.addQueueConfiguration(QueueConfiguration.of(EXPIRY_QUEUE).setRoutingType(RoutingType.ANYCAST));
      config1.addQueueConfiguration(QueueConfiguration.of(DIVERTED_QUEUE).setRoutingType(RoutingType.ANYCAST));

      // Configure server 2
      Configuration config2 = createDefaultNettyConfig()
         .setName("server2")
         .setBindingsDirectory(getBindingsDir(1, false))
         .setJournalDirectory(getJournalDir(1, false))
         .setPagingDirectory(getPageDir(1, false))
         .setLargeMessagesDirectory(getLargeMessagesDir(1, false))
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration("netty", "tcp://localhost:61617");

      config2.addQueueConfiguration(QueueConfiguration.of(SOURCE_QUEUE).setRoutingType(RoutingType.ANYCAST));
      config2.addQueueConfiguration(QueueConfiguration.of(EXPIRY_QUEUE).setRoutingType(RoutingType.ANYCAST));
      config2.addQueueConfiguration(QueueConfiguration.of(DIVERTED_QUEUE).setRoutingType(RoutingType.ANYCAST));

      // Configure expiry for the source queue on both servers
      AddressSettings addressSettings = new AddressSettings()
         .setExpiryAddress(SimpleString.of(EXPIRY_QUEUE))
         .setExpiryDelay(-1L); // Use message TTL

      config1.addAddressSetting(SOURCE_QUEUE, addressSettings);
      config2.addAddressSetting(SOURCE_QUEUE, addressSettings);

      // Add divert from expiry queue to diverted queue on both servers
      DivertConfiguration divertConfiguration = new DivertConfiguration()
         .setName("ExpiryDivert")
         .setAddress(EXPIRY_QUEUE)
         .setForwardingAddress(DIVERTED_QUEUE)
         .setRoutingName("ExpiryDivert");

      config1.addDivertConfiguration(divertConfiguration);
      config2.addDivertConfiguration(divertConfiguration);

      // Configure mirroring from server1 to server2
      config1.addAMQPConnection(
         new org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration()
            .setName("mirror")
            .setUri("tcp://localhost:61617")
            .addElement(new org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement()
               .setDurable(true)));

      // Configure mirroring from server2 to server1
      config2.addAMQPConnection(
         new org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration()
            .setName("mirror")
            .setUri("tcp://localhost:61616")
            .addElement(new org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement()
               .setDurable(true)));

      // Start servers
      server1 = createServer(config1);
      server1.start();

      server2 = createServer(config2);
      server2.start();

      waitForServerToStart(server1);
      waitForServerToStart(server2);
   }

   @AfterEach
   public void tearDown() throws Exception {
      if (server2 != null) {
         server2.stop();
         server2 = null;
      }

      if (server1 != null) {
         server1.stop();
         server1 = null;
      }

      super.tearDown();
   }

   @TestTemplate
   public void testLargeMessageExpiryDivertMirror() throws Exception {
      logger.info("Testing with protocol: {}", protocol);

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue sourceQueue = session.createQueue(SOURCE_QUEUE);
      MessageProducer producer = session.createProducer(sourceQueue);

      // Set a short TTL so messages expire quickly
      producer.setTimeToLive(500); // 500ms

      // Create and send large messages
      byte[] payload = new byte[MESSAGE_SIZE];
      for (int i = 0; i < payload.length; i++) {
         payload[i] = (byte) (i % 256);
      }

      logger.info("Sending {} large messages of size {} bytes", NUMBER_OF_MESSAGES, MESSAGE_SIZE);
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         BytesMessage message = session.createBytesMessage();
         message.writeBytes(payload);
         message.setIntProperty("messageIndex", i);
         producer.send(message);
      }

      session.close();
      connection.close();

      // Wait for messages to expire
      logger.info("Waiting for messages to expire...");
      Thread.sleep(1000);

      // Force expiry scan
      org.apache.activemq.artemis.core.server.Queue sourceQueueOnServer1 = server1.locateQueue(SOURCE_QUEUE);
      org.apache.activemq.artemis.core.server.Queue expiryQueueOnServer1 = server1.locateQueue(EXPIRY_QUEUE);
      org.apache.activemq.artemis.core.server.Queue divertedQueueOnServer1 = server1.locateQueue(DIVERTED_QUEUE);

      // Trigger expiry
      sourceQueueOnServer1.expireReferences();

      // Wait for all messages to be moved to expiry queue
      Wait.assertEquals(NUMBER_OF_MESSAGES, () -> getMessageCount(expiryQueueOnServer1), 5000, 100);
      logger.info("All messages expired to expiry queue on server1");

      // Wait for all messages to be diverted
      Wait.assertEquals(NUMBER_OF_MESSAGES, () -> getMessageCount(divertedQueueOnServer1), 5000, 100);
      logger.info("All messages diverted to diverted queue on server1");

      // Now verify server2 has all the messages mirrored
      org.apache.activemq.artemis.core.server.Queue expiryQueueOnServer2 = server2.locateQueue(EXPIRY_QUEUE);
      org.apache.activemq.artemis.core.server.Queue divertedQueueOnServer2 = server2.locateQueue(DIVERTED_QUEUE);

      assertNotNull(expiryQueueOnServer2, "Expiry queue should exist on server2");
      assertNotNull(divertedQueueOnServer2, "Diverted queue should exist on server2");

      // Wait for mirroring to complete
      Wait.assertEquals(NUMBER_OF_MESSAGES, () -> getMessageCount(expiryQueueOnServer2), 10000, 100);
      Wait.assertEquals(NUMBER_OF_MESSAGES, () -> getMessageCount(divertedQueueOnServer2), 10000, 100);

      logger.info("Verified mirroring - Expiry queue on server2 has {} messages, Diverted queue has {} messages",
                  getMessageCount(expiryQueueOnServer2), getMessageCount(divertedQueueOnServer2));

      // Verify we can consume the messages from server2
      ConnectionFactory factory2 = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61617");
      Connection connection2 = factory2.createConnection();
      connection2.start();

      Session consumerSession = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue divertedQueueJMS = consumerSession.createQueue(DIVERTED_QUEUE);
      MessageConsumer consumer = consumerSession.createConsumer(divertedQueueJMS);

      logger.info("Consuming messages from diverted queue on server2...");
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         BytesMessage message = (BytesMessage) consumer.receive(5000);
         assertNotNull(message, "Should receive message " + i);

         byte[] receivedPayload = new byte[MESSAGE_SIZE];
         int bytesRead = message.readBytes(receivedPayload);
         assertEquals(MESSAGE_SIZE, bytesRead, "Message size should match");

         // Verify payload integrity
         for (int j = 0; j < MESSAGE_SIZE; j++) {
            assertEquals((byte) (j % 256), receivedPayload[j],
               "Payload byte at position " + j + " should match");
         }

         logger.debug("Successfully received and verified message {}", i);
      }

      logger.info("All {} messages successfully consumed from server2", NUMBER_OF_MESSAGES);

      consumerSession.close();
      connection2.close();
   }
}
