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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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
         //configuration.addAddressSetting("ACTIVEMQ_ARTEMIS_MIRROR_toDC2", new AddressSettings().setMaxSizeMessages(0).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));
         //configuration.addAddressSetting("#", new AddressSettings().setMaxSizeMessages(0).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));
         configuration.setResolveProtocols(true);
         configuration.setMirrorAckManagerRetryDelay(100).setMirrorAckManagerPageAttempts(1).setMirrorAckManagerQueueAttempts(1);
         configuration.addAddressConfiguration(new CoreAddressConfiguration().setName(address).addRoutingType(RoutingType.MULTICAST).addQueueConfiguration(QueueConfiguration.of(queueName1).setRoutingType(RoutingType.MULTICAST)).addQueueConfiguration(QueueConfiguration.of(queueName2).setRoutingType(RoutingType.MULTICAST)));
         configuration.addAcceptorConfiguration("mirror", "tcp://localhost:60001?protocols=AMQP;CORE");
         configuration.addAcceptorConfiguration("clients", "tcp://localhost:61616?protocols=AMQP;CORE");
         AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("toDC2", "tcp://localhost:60000").setRetryInterval(100).setReconnectAttempts(-1);
         AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement().setDurable(true);
         brokerConnectConfiguration.addMirror(mirror);
         configuration.addAMQPConnection(brokerConnectConfiguration);
         server1 = createServer(true, configuration);
         server1.setIdentity("server1");
         server1.start();
         //server1.getRemotingService().getAcceptor("mirror").pause();
      }

      ActiveMQServer server2;
      {
         Configuration configuration = createDefaultConfig(1, false);
         configuration.setJournalRetentionDirectory(getTemporaryDir() + "/retention2");
         //configuration.addAddressSetting("ACTIVEMQ_ARTEMIS_MIRROR_toDC1", new AddressSettings().setMaxSizeMessages(0).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));
         //configuration.addAddressSetting("#", new AddressSettings().setMaxSizeMessages(0).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));
         configuration.setResolveProtocols(true);
         configuration.addAddressConfiguration(new CoreAddressConfiguration().setName(address).addRoutingType(RoutingType.MULTICAST).addQueueConfiguration(QueueConfiguration.of(queueName1).setRoutingType(RoutingType.MULTICAST)).addQueueConfiguration(QueueConfiguration.of(queueName2).setRoutingType(RoutingType.MULTICAST)));
         configuration.addAcceptorConfiguration("mirror", "tcp://localhost:60000?protocols=AMQP;CORE");
         configuration.addAcceptorConfiguration("clients", "tcp://localhost:61617?protocols=AMQP;CORE");
         AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("toDC1", "tcp://localhost:60001").setRetryInterval(100).setReconnectAttempts(-1);
         AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement();
         brokerConnectConfiguration.addMirror(mirror);
         configuration.addAMQPConnection(brokerConnectConfiguration);
         server2 = createServer(true, configuration);
         server2.setIdentity("server2");
         server2.start();
         //server2.getRemotingService().getAcceptor("mirror").pause();
      }

      server2.getPostOffice().getAllBindings().filter(binding -> binding instanceof LocalQueueBinding).forEach(b -> {
         System.out.println(b.getAddress());
         System.out.println("Queue :: " + ((LocalQueueBinding)b).getQueue());
      });

      Queue mirrorQueue2 = server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC1");
      //assertNotNull(mirrorQueue2);
      //mirrorQueue2.getPagingStore().startPaging();

      Queue mirrorQueue1 = server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC2");
      //assertNotNull(mirrorQueue1);
      //mirrorQueue1.getPagingStore().startPaging();


      logger.info("\n*******************************************************************************************************************************\n" +
                  "server1 ID is {}\n" +
                  "server2 ID is {}\n" +
                  "*******************************************************************************************************************************", server1.getNodeID(), server2.getNodeID());

      long numberOfMessages = 100;

      ConnectionFactory factory1 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
      ConnectionFactory factory2 = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61617");

      try (Connection connection = factory2.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createTopic(address));
         //Thread.sleep(1000);
         logger.info("Sending {} messages", numberOfMessages);
         for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }
         session.commit();
      }

      try (Connection connection1 = factory1.createConnection();
           Connection connection2 = factory2.createConnection()) {

         connection1.start();
         connection2.start();

         Session session1 = connection1.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer1_server1 = session1.createConsumer(session1.createQueue(address + "::" + queueName1));
         MessageConsumer consumer2_server1 = session1.createConsumer(session1.createQueue(address + "::" + queueName2));
         assertNotNull(consumer1_server1.receive(5000));
         assertNotNull(consumer2_server1.receive(5000));


         Session session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer1_server2 = session2.createConsumer(session1.createQueue(address + "::" + queueName1));
         MessageConsumer consumer2_server2 = session2.createConsumer(session1.createQueue(address + "::" + queueName2));

         for (int i = 0 ; i < numberOfMessages; i++) {
            assertNotNull(consumer1_server2.receive(5000));
            assertNotNull(consumer2_server2.receive(5000));
         }
         session2.commit();

         Wait.assertEquals(0, mirrorQueue1::getMessageCount);
         Wait.assertEquals(0, mirrorQueue2::getMessageCount);


         verifyQueues(server1, server2, 0, queueName1, queueName2);
      }
   }

    private void verifyQueues(ActiveMQServer server1, ActiveMQServer server2, long numberOfMessages, String queueName1, String queueName2) throws Exception {
      Queue queue1Server1 = server1.locateQueue(queueName1);
      Queue queue1Server2 = server2.locateQueue(queueName1);
      Queue queue2Server1 = server1.locateQueue(queueName2);
      Queue queue2Server2 = server2.locateQueue(queueName2);

      Queue mirrorQueue1 = server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC2");
      Queue mirrorQueue2 = server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC1");

      Wait.assertEquals(0L, mirrorQueue1::getMessageCount, 5000, 100);
      Wait.assertEquals(0L, mirrorQueue2::getMessageCount, 5000, 100);

      Wait.assertEquals(numberOfMessages, queue1Server1::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queue1Server2::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queue2Server1::getMessageCount, 5000, 100);
      Wait.assertEquals(numberOfMessages, queue2Server2::getMessageCount, 5000, 100);
   }
}