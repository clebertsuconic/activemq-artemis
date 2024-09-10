/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class AMQPMirrorCompactTest extends AmqpClientTestSupport {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected static final int AMQP_PORT_2 = 5673;


   private AssertionLoggerHandler loggerHandler;

   @BeforeEach
   public void startLogging() {
      loggerHandler = new AssertionLoggerHandler();
   }

   @AfterEach
   public void stopLogging() throws Exception {
      try {
         assertFalse(loggerHandler.findText("AMQ222214"));
      } finally {
         loggerHandler.close();
      }
   }

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      ActiveMQServer server = createServer(AMQP_PORT, false);


      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("anotherServer", "tcp://localhost:" + AMQP_PORT_2).setReconnectAttempts(-1).setRetryInterval(100);
      AMQPMirrorBrokerConnectionElement replica = new AMQPMirrorBrokerConnectionElement().setDurable(true).setMirrorSNF(SimpleString.of("$OUTPUT_QUEUE"));
      replica.setName("theReplica");
      amqpConnection.addElement(replica);
      server.getConfiguration().addAMQPConnection(amqpConnection);


      return server;
   }

   @Test
   public void testCleanup() throws Exception {
      testCleanup(false, false);
   }

   @Test
   public void testCleanupSNFPaging() throws Exception {
      testCleanup(true, false);
   }

   @Test
   public void testCleanupQueuesPaging() throws Exception {
      testCleanup(false, true);
   }

   @Test
   public void testCleanupEverythingPaging() throws Exception {
      testCleanup(true, true);
   }

   @Test
   public void testCleanup(boolean snfPaging, boolean queuesPaging) throws Exception {
      server.start();

      String topicName = "TestReplicaTopic";
      String queueName = "TestReplicaQueue";

      server.addAddressInfo(new AddressInfo(topicName).addRoutingType(RoutingType.MULTICAST));
      server.addAddressInfo(new AddressInfo(queueName).addRoutingType(RoutingType.ANYCAST));
      server.createQueue(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));

      if (snfPaging) {
         server.locateQueue("$OUTPUT_QUEUE").getPagingStore().startPaging();
      }


      if (queuesPaging) {
         server.getPagingManager().getPageStore(SimpleString.of(topicName)).startPaging();
         server.getPagingManager().getPageStore(SimpleString.of(queueName)).startPaging();
      }


      ConnectionFactory factory = CFUtil.createConnectionFactory("amqp", "tcp://localhost:" + AMQP_PORT);
      try (Connection connection = factory.createConnection()) {
         connection.setClientID("clientID1");
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic(topicName);
         session.createDurableSubscriber(topic, "t1");
         session.createDurableSubscriber(topic, "t2");

         Queue queue = session.createQueue(queueName);

         MessageProducer queueProducer = session.createProducer(queue);
         MessageProducer topicProducer = session.createProducer(topic);
         for (int i = 0; i < 1000; i++) {
            queueProducer.send(session.createTextMessage("hello " + i));
            topicProducer.send(session.createTextMessage("hello " + i));
         }

         session.commit();
      }

      System.exit(-1);

   }

}