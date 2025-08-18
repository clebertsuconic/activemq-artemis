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
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.PageFullMessagePolicy;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateConnectionDualMirrorTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testLateConnection() throws Exception {

      String queueName1 = getName() + "1";
      String queueName2 = getName() + "2";
      String address = "TheAddress";

      ActiveMQServer server1;
      {
         Configuration configuration = createDefaultConfig(0, false);
         configuration.setJournalRetentionDirectory(getTemporaryDir() + "/retention1");
         configuration.getAddressConfigurations().clear();
         configuration.addAddressSetting("ACTIVEMQ_ARTEMIS_MIRROR_toDC1", new AddressSettings().setMaxSizeMessages(0).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));
         configuration.setResolveProtocols(true);
         configuration.addAddressConfiguration(new CoreAddressConfiguration().setName(address).addRoutingType(RoutingType.MULTICAST).addQueueConfiguration(QueueConfiguration.of(queueName1).setRoutingType(RoutingType.MULTICAST)).addQueueConfiguration(QueueConfiguration.of(queueName2).setRoutingType(RoutingType.MULTICAST)));
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
         Configuration configuration = createDefaultConfig(1, false);
         configuration.setJournalRetentionDirectory(getTemporaryDir() + "/retention2");
         configuration.addAddressSetting("ACTIVEMQ_ARTEMIS_MIRROR_toDC2", new AddressSettings().setMaxSizeMessages(0).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));
         configuration.setResolveProtocols(true);
         configuration.addAddressConfiguration(new CoreAddressConfiguration().setName(address).addRoutingType(RoutingType.MULTICAST).addQueueConfiguration(QueueConfiguration.of(queueName1).setRoutingType(RoutingType.MULTICAST)).addQueueConfiguration(QueueConfiguration.of(queueName2).setRoutingType(RoutingType.MULTICAST)));
         configuration.addAcceptorConfiguration("mirror", "tcp://localhost:60000?protocols=AMQP;CORE");
         configuration.addAcceptorConfiguration("clients", "tcp://localhost:61617?protocols=AMQP;CORE");
         AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("toDC1", "tcp://localhost:61616").setRetryInterval(100).setReconnectAttempts(-1);
         AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement();
         brokerConnectConfiguration.addMirror(mirror);
         configuration.addAMQPConnection(brokerConnectConfiguration);
         server2 = createServer(true, configuration);
         server2.setIdentity("server2");
         server2.start();
         server2.getRemotingService().getAcceptor("mirror").pause();
      }

      server2.getPostOffice().getAllBindings().filter(binding -> binding instanceof LocalQueueBinding).forEach(b -> {
         System.out.println(b.getAddress());
         System.out.println("Queue :: " + ((LocalQueueBinding)b).getQueue());
      });

      Queue mirrorQueue2 = server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC1");
      Assertions.assertNotNull(mirrorQueue2);
      mirrorQueue2.getPagingStore().startPaging();

      Queue mirrorQueue1 = server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC2");
      Assertions.assertNotNull(mirrorQueue1);
      mirrorQueue1.getPagingStore().startPaging();


      logger.info("\n*******************************************************************************************************************************\n" +
                  "server1 ID is {}\n" +
                  "server2 ID is {}\n" +
                  "*******************************************************************************************************************************", server1.getNodeID(), server2.getNodeID());

      long numberOfMessages = 10;

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61617?protocols=AMQP;useEpoll=false;amqpCredits=1;amqpLowCredits=1");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createTopic(address));
         //Thread.sleep(1000);
         logger.info("Sending {} messages", numberOfMessages);
         for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }
         session.commit();
      }


      logger.info("Restarting...");

      verifyQueues(server1, server2, numberOfMessages, queueName1, queueName2);

      server1.stop();
      server2.start();

      server1.start();
      server2.start();


      verifyQueues(server1, server2, numberOfMessages, queueName1, queueName2);

      System.exit(-1);
   }

   private void verifyQueues(ActiveMQServer server1, ActiveMQServer server2, long numberOfMessages, String queueName1, String queueName2) throws Exception {
      Queue queue1Server1 = server1.locateQueue(queueName1);
      Queue queue1Server2 = server2.locateQueue(queueName1);
      Queue queue2Server1 = server1.locateQueue(queueName2);
      Queue queue2Server2 = server2.locateQueue(queueName2);

      Wait.assertEquals(numberOfMessages, queue1Server1::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queue1Server2::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queue2Server1::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queue2Server2::getMessageCount, 5000, 100);
   }
}