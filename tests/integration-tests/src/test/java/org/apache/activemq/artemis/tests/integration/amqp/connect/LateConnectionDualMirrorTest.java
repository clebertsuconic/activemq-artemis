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
import javax.jms.Session;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateConnectionDualMirrorTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testLateConnection() throws Exception {

      String queueName = getName();

      ActiveMQServer server1;
      {
         Configuration configuration = createDefaultConfig(1, false);
         configuration.setResolveProtocols(true);
         configuration.addQueueConfiguration(QueueConfiguration.of(name).setAddress(queueName).setRoutingType(RoutingType.ANYCAST));
         configuration.addAcceptorConfiguration("clients", "tcp://localhost:61616?protocols=AMQP;CORE");
         AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("toDC2", "tcp://localhost:60000").setRetryInterval(100).setReconnectAttempts(-1);
         AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement().setDurable(true);
         brokerConnectConfiguration.addMirror(mirror);
         configuration.addAMQPConnection(brokerConnectConfiguration);
         server1 = createServer(true, configuration);
         server1.setIdentity("server1");
         server1.start();
      }

      ActiveMQServer server2;
      {
         Configuration configuration = createDefaultConfig(2, false);
         configuration.setResolveProtocols(true);
         configuration.addQueueConfiguration(QueueConfiguration.of(name).setAddress(queueName).setRoutingType(RoutingType.ANYCAST));
         configuration.addAcceptorConfiguration("bridge", "tcp://localhost:60000?protocols=AMQP;CORE");
         configuration.addAcceptorConfiguration("clients", "tcp://localhost:61617?protocols=AMQP;CORE");
         AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("toDC1", "tcp://localhost:61616").setRetryInterval(100).setReconnectAttempts(-1);
         AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement();
         brokerConnectConfiguration.addMirror(mirror);
         configuration.addAMQPConnection(brokerConnectConfiguration);
         server2 = createServer(true, configuration);
         server2.setIdentity("server2");
         server2.start();
         server2.getRemotingService().getAcceptor("bridge").pause();
      }


      logger.info("\n*******************************************************************************************************************************\n" +
                  "server1 ID is {}\n" +
                  "server2 ID is {}\n" +
                  "*******************************************************************************************************************************", server1.getNodeID(), server2.getNodeID());

      long numberOfMessages = 1;

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61617?protocols=AMQP;useEpoll=false;amqpCredits=1;amqpLowCredits=1");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue(queueName));
         //Thread.sleep(1000);
         logger.info("Sending {} messages", numberOfMessages);
         for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }
         session.commit();
      }

      Queue queueServer1 = server1.locateQueue(queueName);
      Queue queueServer2 = server2.locateQueue(queueName);

      Wait.assertEquals(numberOfMessages, queueServer1::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queueServer2::getMessageCount, 5000, 100);

      logger.info("Restarting...");
      server2.getRemotingService().getAcceptor("bridge").reload();

      //Thread.sleep(5000);

      Wait.assertEquals(numberOfMessages, queueServer1::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queueServer2::getMessageCount, 5000, 100);


   }
}