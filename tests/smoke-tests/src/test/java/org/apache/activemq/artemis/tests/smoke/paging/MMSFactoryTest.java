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

package org.apache.activemq.artemis.tests.smoke.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MMSFactoryTest extends SmokeTestBase {

   public static ConnectionFactory createConnectionFactory(String protocol, String uri) {
      if (protocol.toUpperCase().equals("OPENWIRE")) {
         return new org.apache.activemq.ActiveMQConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("AMQP")) {

         if (uri.startsWith("tcp://")) {
            // replacing tcp:// by amqp://
            uri = "amqp" + uri.substring(3);
         }
         return new JmsConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("CORE") || protocol.toUpperCase().equals("ARTEMIS")) {
         return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(uri);
      } else {
         throw new IllegalStateException("Unkown:" + protocol);
      }
   }

   public static final String SERVER_NAME_0 = "paging";

   String protocol = "AMQP";
   AtomicBoolean running = new AtomicBoolean(true);

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      startServer(SERVER_NAME_0, 0, 30000);
   }


   class Consumer extends Thread {

      final int slowTime;

      final String queuename;
      final String name;

      final ReusableLatch slowLatch;
      final AtomicInteger received;

      Consumer(int slowTime, String queueName, ReusableLatch slowLatch, AtomicInteger received) {
         this(slowTime, queueName, slowLatch, received, null);
      }

      Consumer(int slowTime, String queueName, ReusableLatch slowLatch, AtomicInteger received, String name) {
         super (name == null ? "MM Eater " + queueName : name);
         this.name = name;
         this.slowTime = slowTime;
         this.queuename = queueName;
         this.slowLatch = slowLatch;
         this.received = received;
      }

      volatile int errors = 0;

      @Override
      public void run() {

         Connection connection;
         Session session;
         MessageConsumer consumer;
         Queue queue;
         ConnectionFactory factory = createConnectionFactory(protocol, "tcp://localhost:61616");

         try {

            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            queue = session.createQueue(queuename);
            consumer = session.createConsumer(queue);
         } catch (Exception e) {
            e.printStackTrace();
            errors++;
            System.exit(-1);
            return;
         }

         while (running.get()) {
            try {
               Message message = consumer.receive(5000);
               if (message != null) {
                  if (name != null) {
                     System.out.println(name + " processed " + message.getStringProperty("color"));
                  }
                  slowLatch.await(slowTime, TimeUnit.MILLISECONDS);
                  message.acknowledge();
                  received.incrementAndGet();
               }

            } catch (Exception e) {
               e.printStackTrace();
               errors++;
            }
         }
      }
   }


   @Test
   public void testMMSorting() throws Exception {

      AtomicInteger allConsumed = new AtomicInteger(0);
      AtomicInteger redConsumed = new AtomicInteger(0);
      // this is expected to be 0 at the end, we are not sending greens
      AtomicInteger greenConsumed = new AtomicInteger(0);

      final int NUMBER_OF_LIKES_THEM_ALL = 6;
      final int NUMBER_OF_LIKES_GREEN = 6;
      final int NUMBER_OF_LIKES_RED = 6;

      final int PROCESSING_TIME_ALL = 10;
      final int PROCESSING_TIME_RED = 100;

      final int BATCH_SIZE = 5_000;

      ReusableLatch latchSlow = new ReusableLatch(0);

      Consumer[] likesThemAll = new Consumer[NUMBER_OF_LIKES_THEM_ALL];
      Consumer[] likesGreen = new Consumer[NUMBER_OF_LIKES_GREEN];
      Consumer[] likesRed = new Consumer[NUMBER_OF_LIKES_RED];

      for (int i = 0; i < NUMBER_OF_LIKES_THEM_ALL; i++) {
         likesThemAll[i] = new Consumer(PROCESSING_TIME_ALL, "MMFactory::All", latchSlow, allConsumed);
         likesThemAll[i].start();
      }


      for (int i = 0; i < NUMBER_OF_LIKES_GREEN; i++) {
         likesGreen[i] = new Consumer(PROCESSING_TIME_ALL, "MMFactory::Green", latchSlow, greenConsumed);
         likesGreen[i].start();
      }


      for (int i = 0; i < NUMBER_OF_LIKES_RED; i++) {
         likesRed[i] = new Consumer(PROCESSING_TIME_RED, "MMFactory::Red", latchSlow, redConsumed);
         likesRed[i].start();
      }

      Consumer verySlowRed = new Consumer(PROCESSING_TIME_RED * 5, "MMFactory::Red", latchSlow, redConsumed, "Very slow consumer");
      verySlowRed.start();

      ConnectionFactory factory = createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Topic queue = session.createTopic("MMFactory");
      MessageProducer mmsFactory = session.createProducer(queue);

      while (true) {
         // slow down processing
         latchSlow.countUp();
         for (int i = 0; i < BATCH_SIZE; i++) {
            String color = "red"; // we are only making red today!
            TextMessage message = session.createTextMessage("This is a " + color + " MM");
            message.setStringProperty("color", color);
            message.setStringProperty("JMSXGroupID", "" + (i % 20));
            mmsFactory.send(message);
         }
         session.commit();

         for (int i = 0; i < 100; i++) {

            if (allConsumed.get() == BATCH_SIZE) {
               // We have enough.. consume them all now
               break;
            }

            System.out.println("All Processing: " + allConsumed);
            System.out.println("Red Processing: " + redConsumed);
            System.out.println("Green Processing: " + greenConsumed);
            Thread.sleep(1_000);
         }

         // Speed up processing
         latchSlow.countDown();

         Wait.assertEquals(BATCH_SIZE, allConsumed::get);
         Wait.assertEquals(BATCH_SIZE, redConsumed::get);
         Assert.assertEquals(0, greenConsumed.get());

         redConsumed.set(0);
         allConsumed.set(0);

      }

   }

}
