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

package org.apache.activemq.artemis.tests.smoke.mmfactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.jboss.logging.Logger;
import org.junit.Before;
import org.junit.Test;

public class MMSFactoryTest extends SmokeTestBase {

   public static final String SERVER_NAME_0 = "mmfactory";
   private static final Logger logger = Logger.getLogger(MMSFactoryTest.class);
   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 11099;

   String protocol = "AMQP";
   AtomicBoolean running = new AtomicBoolean(true);

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

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      disableCheckThread();
      startServer(SERVER_NAME_0, 0, 30000);
   }

   @Test
   public void testMMSorting() throws Exception {

      JMXConnector jmxConnector = getJmxConnector(JMX_SERVER_HOSTNAME, JMX_SERVER_PORT);

      MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
      ActiveMQServerControl activeMQServerControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);

      ObjectName queueObjectName = objectNameBuilder.getQueueObjectName(SimpleString.toSimpleString("MMFactory"), SimpleString.toSimpleString("MMConsumer"), RoutingType.MULTICAST);
      QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, queueObjectName, QueueControl.class, false);

      AtomicInteger received = new AtomicInteger(0);

      final int BATCH_SIZE = 7_000;
      final int NUMBER_OF_CONSUMERS = 6;
      final int NUMBER_OF_FASTCONSUMERS = 0;

      ReusableLatch valve = new ReusableLatch(0);
      ConcurrentHashSet<Integer> blue = new ConcurrentHashSet<>();
      ConcurrentHashSet<Integer> red = new ConcurrentHashSet<>();
      Consumer[] consumers = new Consumer[NUMBER_OF_CONSUMERS + NUMBER_OF_FASTCONSUMERS];

      CyclicBarrier barrier = new CyclicBarrier(consumers.length + 1);

      for (int i = 0; i < NUMBER_OF_CONSUMERS; i++) {
         consumers[i] = new Consumer(barrier, (i % 2 == 0 ? 200 : 500), "MMFactory::MMConsumer", blue, red, received, valve, 100, "Consumer " + i);
         consumers[i].start();
      }

      for (int i = NUMBER_OF_CONSUMERS; i < NUMBER_OF_CONSUMERS + NUMBER_OF_FASTCONSUMERS; i++) {
         consumers[i] = new Consumer(barrier, 0, "MMFactory::MMConsumer", blue, red, received, valve, 100, "Fast Consumer " + i);
         consumers[i].start();
      }

      barrier.await(); // align everybody after consumers are all connected

      int expectedTotalSize = 0;

      ConnectionFactory factory = createConnectionFactory(protocol, "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic queue = session.createTopic("MMFactory");
         MessageProducer mmsFactory = session.createProducer(queue);

         String largeString;
         {
            StringBuffer largeStringBuffer = new StringBuffer();

            while (largeStringBuffer.length() < 10) {
               largeStringBuffer.append(RandomUtil.randomString());
            }
            largeString = largeStringBuffer.toString();
         }

         try {
            while (true) {
               received.set(0);
               valve.countUp();
               for (int i = 0; i < BATCH_SIZE; i++) {
                  if (i == BATCH_SIZE / 2) {
                     System.out.println("Reconnecting...");
                     for (int r = 0; r < consumers.length; r++) {
                        consumers[r].reconnect();
                     }
                     System.out.println("*** Consumed = " + received.get() + ", sent=" + i);
                  } else if (i % 200 == 0) {
                     int r = RandomUtil.randomInterval(0, consumers.length - 1);
                     //consumers[r].reconnect();
                     System.out.println("Consumed = " + received.get() + ", sent=" + i);
                     for (Consumer c : consumers) {
                        c.reconnect();
                     }
                  }
                  TextMessage message = session.createTextMessage("This is blue " + largeString);
                  message.setStringProperty("color", "blue");
                  message.setIntProperty("i", i);
                  mmsFactory.send(message);

                  message = session.createTextMessage("This is red " + largeString);
                  message.setStringProperty("color", "red");
                  message.setIntProperty("i", i);
                  mmsFactory.send(message);
               }
               valve.countDown(); // after producing is done, we just flush everything and check the results

               Wait.assertTrue(() -> {
                  // We will wait a bit here until it's a least a bit closer to the whole Batch
                  if (received.get() > (BATCH_SIZE * 2 - 500)) {
                     return true;
                  } else {
                     System.out.println("Received " + received.get());
                     return false;
                  }
               }, 45_000, 1_000);

               Wait.assertTrue("Blues are missing", () -> checkAllReceived("blue", BATCH_SIZE, blue), 30_000, 1_000);
               Wait.assertTrue("Reds are missing", () -> checkAllReceived("red", BATCH_SIZE, red), 30_000, 1_000);
               red.clear();
               blue.clear();

               expectedTotalSize += BATCH_SIZE * 2;
               Wait.assertEquals(expectedTotalSize, queueControl::getMessagesAdded);
               Wait.assertEquals(expectedTotalSize, queueControl::getMessagesAcknowledged);
            }
         } finally {
            for (Consumer c : consumers) {
               c.disconnect();
            }
         }
      }

   }

   boolean checkAllReceived(String color, int batchSize, Set<Integer> values) {
      System.out.println("*******************************************************************************************************************************");
      System.out.println("Checks on " + color);
      for (int i = 0; i < batchSize; i++) {
         if (!values.contains(i)) {
            System.out.println(color + "[" + i + "] is missing");
            System.out.println("-------------------------------------------------------------------------------------------------------------------------------");
            return false;
         }
      }
      System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
      return true;
   }

   private void printCounters(String where,
                              AtomicInteger allConsumed,
                              AtomicInteger redConsumed,
                              AtomicInteger greenConsumed,
                              AtomicInteger slowCounter) {
      System.out.println("*******************************************************************************************************************************");
      System.out.println("*  " + where + "   *");
      System.out.println("All Processing: " + allConsumed);
      System.out.println("Red Processing: " + redConsumed);
      System.out.println("Green Processing: " + greenConsumed);
      System.out.println("Slow counter " + slowCounter);
      System.out.println("*******************************************************************************************************************************");
   }

   class Consumer extends Thread {

      final int slowTime;

      final String queuename;
      final ReusableLatch valve;
      final int credits;
      final Set<Integer> blue;
      final Set<Integer> red;
      final AtomicInteger received;
      final CyclicBarrier barrier;
      ConnectionFactory factory;
      volatile boolean disconnecting = false;
      volatile boolean reconnecting = false;
      Connection connection;
      Session session;
      MessageConsumer consumer;
      Queue queue;
      volatile int errors = 0;

      Consumer(CyclicBarrier barrier,
               int slowTime,
               String queueName,
               Set<Integer> blue,
               Set<Integer> red,
               AtomicInteger received,
               ReusableLatch valve,
               int credits,
               String name) {
         this.slowTime = slowTime;
         this.queuename = queueName;
         this.blue = blue;
         this.red = red;
         this.received = received;
         this.valve = valve;
         this.credits = credits;
         this.barrier = barrier;
      }

      public void reconnect() {
         reconnecting = true;
         try {
            if (connection != null) {
               connection.close();
            }
         } catch (Throwable e) {
            e.printStackTrace();
         }

      }

      public void disconnect() {
         try {
            disconnecting = true;
            connection.close();
         } catch (Exception e) {
            e.printStackTrace();
            errors++;
         }
      }

      @Override
      public void run() {

         //factory = createConnectionFactory(protocol, "tcp://localhost:61616?jms.prefetchPolicy.queuePrefetch=" + credits);
         factory = createConnectionFactory(protocol, "tcp://localhost:61616");

         if (!connect()) {
            System.out.println("Could not connect on " + Thread.currentThread().getName());
            return;
         }

         try {
            barrier.await();
         } catch (Exception e) {
         }

         System.out.println("Starting");

         while (running.get()) {
            try {
               if (reconnecting) {
                  System.out.println("Reconnecting");
                  reconnecting = false;
                  connect();
               }
               Message message = consumer.receive(1000);
               if (message != null) {
                  if (slowTime > 0) {
                     valve.await(slowTime, TimeUnit.MILLISECONDS);
                  }

                  message.acknowledge();

                  received.incrementAndGet();

                  String color = message.getStringProperty("color");
                  int messageSequence = message.getIntProperty("i");

                  switch (color) {
                     case "red":
                        red.add(messageSequence);
                        break;
                     case "blue":
                        blue.add(messageSequence);
                        break;
                  }
               } else {
                  System.out.println(Thread.currentThread().getName() + " is null");
               }
            } catch (Throwable e) {
               if (disconnecting) {
                  return;
               } else {
                  if (!reconnecting) {
                     e.printStackTrace();
                     errors++;
                  }
               }
            }
         }
      }

      private boolean connect() {
         try {
            if (connection != null) {
               connection.close();
            }

            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            queue = session.createQueue(queuename);
            consumer = session.createConsumer(queue);
         } catch (Exception e) {
            if (reconnecting) {
               return true;
            } else {
               e.printStackTrace();
               errors++;
               return false;
            }
         }
         return true;
      }
   }

}
