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
package org.apache.activemq.artemis.tests.integration.openwire.amq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * adapted from: org.apache.activemq.JmsQueueBrowserTest
 */
public class JmsQueueBrowserTest extends BasicOpenWireTest {

   /**
    * Tests the queue browser. Browses the messages then the consumer tries to receive them. The messages should still
    * be in the queue even when it was browsed.
    */
   @Test
   public void testReceiveBrowseReceive() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = (ActiveMQQueue) this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
      MessageProducer producer = session.createProducer(destination);
      MessageConsumer consumer = session.createConsumer(destination);
      connection.start();

      Message[] outbound = new Message[]{session.createTextMessage("First Message"), session.createTextMessage("Second Message"), session.createTextMessage("Third Message")};

      // lets consume any outstanding messages from previous test runs
      while (consumer.receive(1000) != null) {
      }

      producer.send(outbound[0]);
      producer.send(outbound[1]);
      producer.send(outbound[2]);

      // Get the first.
      assertEquals(outbound[0], consumer.receive(1000));
      consumer.close();

      QueueBrowser browser = session.createBrowser(destination);
      Enumeration<?> enumeration = browser.getEnumeration();

      // browse the second
      assertTrue(enumeration.hasMoreElements(), "should have received the second message");
      assertEquals(outbound[1], enumeration.nextElement());

      // browse the third.
      assertTrue(enumeration.hasMoreElements(), "Should have received the third message");
      assertEquals(outbound[2], enumeration.nextElement());

      // There should be no more.
      boolean tooMany = false;
      while (enumeration.hasMoreElements()) {
         tooMany = true;
      }
      assertFalse(tooMany);
      browser.close();

      // Re-open the consumer.
      consumer = session.createConsumer(destination);
      // Receive the second.
      assertEquals(outbound[1], consumer.receive(1000));
      // Receive the third.
      assertEquals(outbound[2], consumer.receive(1000));
      consumer.close();
   }

   @Test
   public void testBatchSendBrowseReceive() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = (ActiveMQQueue) this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
      MessageProducer producer = session.createProducer(destination);
      MessageConsumer consumer = session.createConsumer(destination);
      connection.start();

      TextMessage[] outbound = new TextMessage[10];
      for (int i = 0; i < 10; i++) {
         outbound[i] = session.createTextMessage(i + " Message");
      }

      // lets consume any outstanding messages from previous test runs
      while (consumer.receive(1000) != null) {
      }
      consumer.close();

      for (int i = 0; i < outbound.length; i++) {
         producer.send(outbound[i]);
      }

      QueueBrowser browser = session.createBrowser(destination);
      Enumeration<?> enumeration = browser.getEnumeration();

      for (int i = 0; i < outbound.length; i++) {
         assertTrue(enumeration.hasMoreElements(), "should have a");
         assertEquals(outbound[i], enumeration.nextElement());
      }
      browser.close();

      for (int i = 0; i < outbound.length; i++) {
         producer.send(outbound[i]);
      }

      // verify second batch is visible to browse
      browser = session.createBrowser(destination);
      enumeration = browser.getEnumeration();
      for (int j = 0; j < 2; j++) {
         for (int i = 0; i < outbound.length; i++) {
            assertTrue(enumeration.hasMoreElements(), "should have a");
            assertEquals(outbound[i].getText(), ((TextMessage) enumeration.nextElement()).getText(), "j=" + j + ", i=" + i);
         }
      }
      browser.close();

      consumer = session.createConsumer(destination);
      for (int i = 0; i < outbound.length * 2; i++) {
         assertNotNull(consumer.receive(2000), "Got message: " + i);
      }
      consumer.close();
   }

   /* disable this test because it uses management mbeans which we don't implement yet.
   @Test
   public void testBatchSendJmxBrowseReceive() throws Exception
   {
      Session session = connection.createSession(false,
            Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = new ActiveMQQueue("TEST");
      MessageProducer producer = session.createProducer(destination);
      MessageConsumer consumer = session.createConsumer(destination);
      connection.start();

      TextMessage[] outbound = new TextMessage[10];
      for (int i = 0; i < 10; i++)
      {
         outbound[i] = session.createTextMessage(i + " Message");
      }
      ;

      // lets consume any outstanding messages from previous test runs
      while (consumer.receive(1000) != null)
      {
      }
      consumer.close();

      for (int i = 0; i < outbound.length; i++)
      {
         producer.send(outbound[i]);
      }

      ObjectName queueViewMBeanName = new ObjectName(
            "org.apache.activemq.artemis:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");

      System.out.println("Create QueueView MBean...");
      QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
            .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

      long concount = proxy.getConsumerCount();
      System.out.println("Consumer Count :" + concount);
      long messcount = proxy.getQueueSize();
      System.out.println("current number of messages in the queue :"
            + messcount);

      // lets browse
      CompositeData[] compdatalist = proxy.browse();
      if (compdatalist.length == 0)
      {
         fail("There is no message in the queue:");
      }
      String[] messageIDs = new String[compdatalist.length];

      for (int i = 0; i < compdatalist.length; i++)
      {
         CompositeData cdata = compdatalist[i];

         if (i == 0)
         {
            System.out.println("Columns: " + cdata.getCompositeType().keySet());
         }
         messageIDs[i] = (String) cdata.get("JMSMessageID");
         System.out.println("message " + i + " : " + cdata.values());
      }

      TabularData table = proxy.browseAsTable();
      System.out.println("Found tabular data: " + table);
      assertTrue("Table should not be empty!", table.size() > 0);

      assertEquals("Queue size", outbound.length, proxy.getQueueSize());
      assertEquals("Queue size", outbound.length, compdatalist.length);
      assertEquals("Queue size", outbound.length, table.size());

      System.out.println("Send another 10");
      for (int i = 0; i < outbound.length; i++)
      {
         producer.send(outbound[i]);
      }

      System.out.println("Browse again");

      messcount = proxy.getQueueSize();
      System.out.println("current number of messages in the queue :"
            + messcount);

      compdatalist = proxy.browse();
      if (compdatalist.length == 0)
      {
         fail("There is no message in the queue:");
      }
      messageIDs = new String[compdatalist.length];

      for (int i = 0; i < compdatalist.length; i++)
      {
         CompositeData cdata = compdatalist[i];

         if (i == 0)
         {
            System.out.println("Columns: " + cdata.getCompositeType().keySet());
         }
         messageIDs[i] = (String) cdata.get("JMSMessageID");
         System.out.println("message " + i + " : " + cdata.values());
      }

      table = proxy.browseAsTable();
      System.out.println("Found tabular data: " + table);
      assertTrue("Table should not be empty!", table.size() > 0);

      assertEquals("Queue size", outbound.length * 2, proxy.getQueueSize());
      assertEquals("Queue size", outbound.length * 2, compdatalist.length);
      assertEquals("Queue size", outbound.length * 2, table.size());

      consumer = session.createConsumer(destination);
      for (int i = 0; i < outbound.length * 2; i++)
      {
         assertNotNull("Got message: " + i, consumer.receive(2000));
      }
      consumer.close();
   }

*/

   @Test
   public void testBrowseReceive() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = (ActiveMQQueue) this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      connection.start();

      // create consumer
      MessageConsumer consumer = session.createConsumer(destination);
      // lets consume any outstanding messages from previous test runs
      while (consumer.receive(1000) != null) {
      }
      consumer.close();

      Message[] outbound = new Message[]{session.createTextMessage("First Message"), session.createTextMessage("Second Message"), session.createTextMessage("Third Message")};

      MessageProducer producer = session.createProducer(destination);
      producer.send(outbound[0]);

      // create browser first
      QueueBrowser browser = session.createBrowser(destination);

      Enumeration<?> enumeration = browser.getEnumeration();

      // browse the first message
      assertTrue(enumeration.hasMoreElements(), "should have received the first message");
      assertEquals(outbound[0], enumeration.nextElement());


      consumer = session.createConsumer(destination);
      // Receive the first message.
      assertEquals(outbound[0], consumer.receive(1000));
      consumer.close();
      browser.close();
      producer.close();
   }

   @Test
   public void testLargeNumberOfMessages() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = (ActiveMQQueue) this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
      connection.start();

      MessageProducer producer = session.createProducer(destination);

      int numberOfMessages = 4096;

      for (int i = 0; i < numberOfMessages; i++) {
         producer.send(session.createTextMessage("Message: " + i));
      }

      QueueBrowser browser = session.createBrowser(destination);
      Enumeration<?> enumeration = browser.getEnumeration();

      assertTrue(enumeration.hasMoreElements());

      int numberBrowsed = 0;

      while (enumeration.hasMoreElements()) {
         Message browsed = (Message) enumeration.nextElement();


         numberBrowsed++;
      }

      assertEquals(numberOfMessages, numberBrowsed);
      browser.close();
      producer.close();
   }


   @Test
   @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
   public void testTXLargeCount() throws Exception {
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      ActiveMQQueue destination = (ActiveMQQueue) this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
      connection.start();

      MessageProducer producer = session.createProducer(destination);

      int numberOfMessages = 4096;

      for (int i = 0; i < numberOfMessages; i++) {
        TextMessage message = session.createTextMessage("Message: " + i);
        message.setIntProperty("i", i);
        producer.send(message);
      }
      session.commit();

      MessageConsumer consumer = session.createConsumer(destination);

      for (int i = 0; i < numberOfMessages; i++) {
         TextMessage message = (TextMessage) consumer.receive(1000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("i"));
      }
      session.commit();
      assertNull(consumer.receiveNoWait());

   }

   /*
   @Test
   public void testQueueBrowserWith2Consumers() throws Exception
   {
      final int numMessages = 1000;
      connection.setAlwaysSyncSend(false);
      Session session = connection.createSession(false,
            Session.CLIENT_ACKNOWLEDGE);
      ActiveMQQueue destination = (ActiveMQQueue) this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);
      ActiveMQQueue destinationPrefetch10 = new ActiveMQQueue(
            "TEST?jms.prefetchSize=10");
      ActiveMQQueue destinationPrefetch1 = new ActiveMQQueue(
            "TEST?jms.prefetchsize=1");
      connection.start();

      ActiveMQConnection connection2 = (ActiveMQConnection) factory
            .createConnection();
      connection2.start();

      Session session2 = connection2.createSession(false,
            Session.AUTO_ACKNOWLEDGE);

      MessageProducer producer = session.createProducer(destination);
      MessageConsumer consumer = session.createConsumer(destinationPrefetch10);

      // lets consume any outstanding messages from previous test runs
      while (consumer.receive(1000) != null)
      {
      }
      consumer.close();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage message = session.createTextMessage("Message: " + i);
         producer.send(message);
      }

      QueueBrowser browser = session2.createBrowser(destinationPrefetch1);
      @SuppressWarnings("unchecked")
      Enumeration<Message> browserView = browser.getEnumeration();

      List<Message> messages = new ArrayList<>();
      for (int i = 0; i < numMessages; i++)
      {
         Message m1 = consumer.receive(5000);
         assertNotNull("m1 is null for index: " + i, m1);
         messages.add(m1);
      }

      int i = 0;
      for (; i < numMessages && browserView.hasMoreElements(); i++)
      {
         Message m1 = messages.get(i);
         Message m2 = browserView.nextElement();
         assertNotNull("m2 is null for index: " + i, m2);
         assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
      }

      // currently browse max page size is ignored for a queue browser consumer
      // only guarantee is a page size - but a snapshot of pagedinpending is
      // used so it is most likely more
      assertTrue("got at least our expected minimum in the browser: ", i > 200);

      assertFalse("nothing left in the browser", browserView.hasMoreElements());
      assertNull("consumer finished", consumer.receiveNoWait());
      connection2.close();
   }
   */

   @Test
   public void testBrowseClose() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQQueue destination = (ActiveMQQueue) this.createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      connection.start();

      TextMessage[] outbound = new TextMessage[]{session.createTextMessage("First Message"), session.createTextMessage("Second Message"), session.createTextMessage("Third Message")};

      // create consumer
      MessageConsumer consumer = session.createConsumer(destination);
      // lets consume any outstanding messages from previous test runs
      while (consumer.receive(1000) != null) {
      }
      consumer.close();

      MessageProducer producer = session.createProducer(destination);
      producer.send(outbound[0]);
      producer.send(outbound[1]);
      producer.send(outbound[2]);

      // create browser first
      QueueBrowser browser = session.createBrowser(destination);
      Enumeration<?> enumeration = browser.getEnumeration();

      // browse some messages
      assertEquals(outbound[0], enumeration.nextElement());
      assertEquals(outbound[1], enumeration.nextElement());
      // assertEquals(outbound[2], (Message) enumeration.nextElement());

      browser.close();

      consumer = session.createConsumer(destination);
      // Receive the first message.
      TextMessage msg = (TextMessage) consumer.receive(1000);
      assertEquals(outbound[0], msg, "Expected " + outbound[0].getText() + " but received " + msg.getText());
      msg = (TextMessage) consumer.receive(1000);
      assertEquals(outbound[1], msg, "Expected " + outbound[1].getText() + " but received " + msg.getText());
      msg = (TextMessage) consumer.receive(1000);
      assertEquals(outbound[2], msg, "Expected " + outbound[2].getText() + " but received " + msg.getText());

      consumer.close();
      producer.close();
   }

}
