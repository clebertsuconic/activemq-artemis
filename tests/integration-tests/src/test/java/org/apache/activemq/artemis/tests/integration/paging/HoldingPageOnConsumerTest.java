/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Assert;
import org.junit.Test;

public class HoldingPageOnConsumerTest extends ActiveMQTestBase {

   ActiveMQServer server;

   @Override
   public void setUp() throws Exception {
      super.setUp();
      final Configuration config = createDefaultConfig(0, true);
      final int PAGE_MAX = 20 * 1024;

      final int PAGE_SIZE = 10 * 1024;

      server = createServer(true, config, PAGE_SIZE, PAGE_MAX);
      server.getConfiguration().getAddressesSettings().clear();
      server.getConfiguration().getAddressesSettings().put("#", new AddressSettings().setMaxDeliveryAttempts(-1).setRedeliveryDelay(0));
      server.start();
   }

   @Test
   public void testHoldingPage() throws Exception {

      server.addAddressInfo(new AddressInfo("theAddres").addRoutingType(RoutingType.MULTICAST));
      server.createQueue(new QueueConfiguration("q1").setAddress("theAddress").setRoutingType(RoutingType.MULTICAST).setDurable(true));
      server.createQueue(new QueueConfiguration("q2").setAddress("theAddress").setRoutingType(RoutingType.MULTICAST).setDurable(true));

      Queue queue = server.locateQueue("q1");
      Assert.assertNotNull(queue);

      queue.getPagingStore().startPaging();

      AmqpClient client = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = client.connect();
      AmqpSession session = connection.createSession();

      AmqpReceiver holdingConsumer = session.createReceiver("theAddress::q1");
      AmqpSender producer = session.createSender("theAddress");

      holdingConsumer.flow(15);

      Assert.assertTrue(queue.getPagingStore().isPaging());

      for (int i = 0; i < 100; i++) {

         if (i == 5 || i == 10 || i == 15) {
            queue.getPagingStore().forceAnotherPage();
         }

         AmqpMessage message = new AmqpMessage();
         message.setDurable(true);
         message.setText("hello");
         message.setApplicationProperty("i", i);
         producer.send(message);

         if (i < 5) {
            AmqpMessage recMessage = holdingConsumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(recMessage);
            recMessage.accept();
         }
      }

      AmqpReceiver receiveAll = session.createReceiver("theAddress::q2");
      receiveAll.flow(100);
      for (int i = 0; i < 100; i++) {
         AmqpMessage message = receiveAll.receive(5, TimeUnit.SECONDS);
         message.accept();
      }

      AmqpReceiver anotherReceiver = session.createReceiver("theAddress::q1");
      anotherReceiver.flow(85);

      for (int i = 0; i < 85; i++) {
         AmqpMessage message = anotherReceiver.receive(5, TimeUnit.SECONDS);
         Assert.assertNotNull(message);
         message.accept();
      }

      queue.getPageSubscription().cleanupEntries(true);

      Assert.assertTrue(queue.getPagingStore().isPaging());

      holdingConsumer.close();

      holdingConsumer = session.createReceiver("theAddress::q1");
      holdingConsumer.flow(10);

      for (int i = 0; i < 10; i++) {

         AmqpMessage message = holdingConsumer.receive(5, TimeUnit.SECONDS);
         Assert.assertNotNull(message);
         message.accept();
      }

      connection.close();

   }

   @Test
   public void testRedeliveryLive() throws Exception {

      server.addAddressInfo(new AddressInfo("theAddres").addRoutingType(RoutingType.MULTICAST));
      server.createQueue(new QueueConfiguration("q1").setAddress("theAddress").setRoutingType(RoutingType.MULTICAST).setDurable(true));


      for (int repeatNumber = 0; repeatNumber < 2; repeatNumber++) {

         Queue queue = server.locateQueue("q1");
         Assert.assertNotNull(queue);

         AmqpClient client = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
         AmqpConnection connection = client.connect();

         try {

            AmqpSession sessionProducer = connection.createSession();
            AmqpSender producer = sessionProducer.createSender("theAddress");

            AmqpSession sessionConsumer = connection.createSession();
            AmqpReceiver consumerFlowing = sessionConsumer.createReceiver("theAddress::q1");
            consumerFlowing.setPresettle(true);

            AmqpSession sessionHolding = connection.createSession();
            AmqpReceiver holdingConsumer = sessionHolding.createReceiver("theAddress::q1");
            holdingConsumer.setPresettle(true);

            queue.getPagingStore().startPaging();
            Assert.assertTrue(queue.getPagingStore().isPaging());

            sessionProducer.begin();
            for (int i = 0; i < 10; i++) {

               AmqpMessage message = new AmqpMessage();
               message.setDurable(true);
               message.setText("hello");
               message.setApplicationProperty("i", i);
               producer.send(message);
            }
            sessionProducer.commit();

            sessionHolding.begin();
            holdingConsumer.flow(1);
            AmqpMessage heldMessage = holdingConsumer.receive(1, TimeUnit.MINUTES);
            heldMessage.accept();
            Assert.assertNotNull(heldMessage);

            //queue.getPagingStore().forceAnotherPage();

            for (int i = 10; i < 20; i++) {
               AmqpMessage message = new AmqpMessage();
               message.setDurable(true);
               message.setText("hello");
               message.setApplicationProperty("i", i);
               producer.send(message);
            }

            consumerFlowing.flow(19);
            sessionConsumer.begin();

            for (int i = 0; i < 19; i++) {

               AmqpMessage messageReceived = consumerFlowing.receive(1, TimeUnit.MINUTES);
               Assert.assertNotNull(messageReceived);
               messageReceived.accept();
            }
            sessionConsumer.commit();

            if (repeatNumber == 0) {
               sessionHolding.commit();
            } else {
               sessionHolding.rollback();
               //sessionHolding.close();
            }


            //Thread.sleep(1000);
            //queue.getPagingStore().getCursorProvider().clearCache();
            queue.getPagingStore().getCursorProvider().cleanup();
            if (repeatNumber == 0) {
               Assert.assertFalse(queue.getPagingStore().isPaging());
            } else {
               Assert.assertTrue(queue.getPagingStore().isPaging());
            }
         } finally {
            connection.close();
         }

         server.stop();
         server.start();
      }

   }


   @Test
   public void testLateDeliveryNoCleanup() throws Exception {

      try {

         server.addAddressInfo(new AddressInfo("theAddres").addRoutingType(RoutingType.MULTICAST));
         server.createQueue(new QueueConfiguration("q1").setAddress("theAddress").setRoutingType(RoutingType.MULTICAST).setDurable(true));

         ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");

         Connection repeatConnection = createRepeatingConnection(factory);
         repeatConnection.start();

         Connection connection = factory.createConnection();
         Session holdingSession = connection.createSession(true, Session.SESSION_TRANSACTED);

         Topic topic = holdingSession.createTopic("theAddress");
         MessageProducer holdingProducer = holdingSession.createProducer(topic);
         Queue queue = server.locateQueue("q1");
         Assert.assertNotNull(queue);

         queue.getPagingStore().startPaging();

         for (int i = 0; i < 10; i++) {
            TextMessage msg = holdingSession.createTextMessage("hello " + i);
            msg.setIntProperty("i", i);
            holdingProducer.send(msg);
         }

         Session clearSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer clearProducer = clearSession.createProducer(topic);
         for (int i = 11; i < 30; i++) {
            TextMessage msg = holdingSession.createTextMessage("hello " + i);
            msg.setIntProperty("i", i);
            clearProducer.send(msg);

            if (i == 20) {
               queue.getPagingStore().forceAnotherPage();
            }
         }

         connection.start();
         Session sessionConsumer = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = sessionConsumer.createConsumer(sessionConsumer.createQueue("theAddress::q1"));
         for (int repeat = 0; repeat < 5; repeat++) {
            for (int i = 11; i < 30; i++) {
               TextMessage msg = (TextMessage) consumer.receive(50000);
               System.out.println("Received " + msg.getIntProperty("i"));
               Assert.assertNotNull(msg);
            }
            if (repeat < 4) {
               sessionConsumer.rollback();
            } else {
               sessionConsumer.commit();
            }
         }

         queue.getPagingStore().getCursorProvider().cleanup();

         Assert.assertTrue(queue.getPagingStore().isPaging());

         //repeatConnection.start();
         holdingSession.commit();

         for (int i = 0; i < 10; i++) {
            TextMessage msg = (TextMessage) consumer.receive(500000);
            System.out.println("Received " + i);
            Assert.assertNotNull(msg);
         }
         sessionConsumer.commit();

         Wait.assertFalse(queue.getPagingStore()::isPaging);

         repeatConnection.close();
         connection.close();
      } catch (Throwable e) {
         e.printStackTrace();
         System.exit(-1);
         throw e;

      }

   }

   private Connection createRepeatingConnection(ConnectionFactory factory) throws JMSException {
      Connection repeatConnection = factory.createConnection();
      Session repeatingSession = repeatConnection.createSession(true, Session.SESSION_TRANSACTED);
      for (int i = 0; i < 100; i++) {
         final int theId = i;
         javax.jms.Queue jmsq1 = repeatingSession.createQueue("theAddress::q1");
         MessageConsumer repeatingConsumer = repeatingSession.createConsumer(jmsq1);
         repeatingConsumer.setMessageListener((message -> {
            try {
               System.out.println(theId + "::Repeating " + message.getIntProperty("i"));
               repeatingSession.rollback();
            } catch (Exception e) {
               e.getMessage();
            }
         }));
      }
      repeatConnection.start();
      return repeatConnection;
   }

}
