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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicatedPageCounterWithSubscriptionsTest extends SmokeTestBase {

   private static final String JMX_SERVER_HOSTNAME = "localhost";

   public static final String SERVER_NAME_0 = "replicated-static0";
   public static final String SERVER_NAME_1 = "replicated-static1";

   private static final int JMX_SERVER0_PORT = 10099;
   private static final int JMX_SERVER1_PORT = 11099;

   private SimpleString ADDRESS = new SimpleString("ReplicatedPageCounterTestMulticast");

   Process server0;
   Process server1;

   @Before
   public void before() throws Exception {
      disableCheckThread();
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      server0 = startServer(SERVER_NAME_0, 0, 30000);
      server1 = startServer(SERVER_NAME_1, 0, 30000);
   }


   public ConnectionFactory createConnectionFactory(String protocol) {

      if (protocol.toUpperCase().equals("AMQP")) {
         return new JmsConnectionFactory("failover:(amqp://localhost:61616,amqp://localhost:61617)?failover.maxReconnectAttempts=16&jms.prefetchPolicy.all=5&jms.forceSyncSend=true");
      } else {
         return new ActiveMQConnectionFactory("tcp://localhost:61616?ha=true&retryInterval=1000&retryIntervalMultiplier=1.0&reconnectAttempts=-1");
      }
   }

   AtomicBoolean running = new AtomicBoolean(true);
   AtomicBoolean draining = new AtomicBoolean(false);

   class ConsumerThread extends Thread {
      final int consumerNR;
      final int processTime;
      final String protocol;
      final CyclicBarrier barrier;

      public ConsumerThread(String protocol, int consumerNR, int processTime, CyclicBarrier barrier) {
         super("ConsumerThread number " + consumerNR + " processTime=" + processTime);
         this.consumerNR = consumerNR;
         this.processTime = processTime;
         this.protocol = protocol;
         this.barrier = barrier;
      }

      public void run() {
         try {
            ConnectionFactory factory = createConnectionFactory(protocol);
            Connection connection = factory.createConnection();
            connection.setClientID("consumerNR_" + consumerNR);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(ADDRESS.toString());
            MessageConsumer consumer = session.createDurableConsumer(topic, "nr_" + consumerNR);
            connection.start();
            barrier.await(10, TimeUnit.SECONDS);
            int messages = 0;
            while (running.get()) {
               try {
                  Message message = consumer.receive(250);
                  if (message != null) {
                     if (messages++ % 1000 == 0) {
                        System.out.println("processed " + messages + " on " + Thread.currentThread().getName() + " draining = " + draining.get());
                     }
                     if (processTime != 0 && !draining.get()) {
                        Thread.sleep(processTime);
                     }
                  }
               } catch (Exception e) {
                  connection.close();

                  if (protocol.toUpperCase().equals("AMQP")) {
                     factory = new JmsConnectionFactory("amqp://localhost:61617");
                  } else {
                     factory = new ActiveMQConnectionFactory("tcp://localhost:61617?ha=true&retryInterval=1000&retryIntervalMultiplier=1.0&reconnectAttempts=-1");
                  }

                  connection = factory.createConnection();
                  connection.setClientID("consumerNR_" + consumerNR);
                  session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                  connection.start();
                  consumer = session.createDurableConsumer(topic, "nr_" + consumerNR);

                  System.err.println("Retrying exception: " + e);
                  e.printStackTrace();
               }
            }
         } catch (Exception e) {
            System.err.println("Fatal error: " + e);
            e.printStackTrace();
         }
      }
   }


   @Test
   public void testMultipleSubscriptions() throws Exception {
      String protocol = "CORE";
      // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
      System.setProperty("java.rmi.server.hostname", JMX_SERVER_HOSTNAME);

      final int MESSAGE_COUNT = 40000;

      MBeanServerConnection mBeanServerConnection = connectJMX(JMX_SERVER1_PORT);
      String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);

      final ActiveMQServerControl activeMQServerControl;
      ActiveMQServerControl tmpControl = null;
      for (int i = 0; i < 10; i++) {
         try {
            tmpControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);
            Wait.assertTrue(tmpControl::isReplicaSync);
            break;
         } catch (Exception e) {
            Thread.sleep(500);
         }
      }
      activeMQServerControl = tmpControl;

      ArrayList<ConsumerThread> threads = new ArrayList<>();

      int SLOW_THREADS = 15;
      int FAST_THREADS = 15;

      CyclicBarrier barrier = new CyclicBarrier(SLOW_THREADS + FAST_THREADS + 1);

      for (int i = 0; i < SLOW_THREADS; i++) {
         threads.add(new ConsumerThread(protocol, i, RandomUtil.randomInterval(5, 100), barrier));
      }
      for (int i = SLOW_THREADS; i < (SLOW_THREADS + FAST_THREADS); i++) {
         threads.add(new ConsumerThread(protocol, i, 0, barrier));
      }

      threads.forEach((t) -> t.start());

      barrier.await(10, TimeUnit.SECONDS);

      ConnectionFactory factory = createConnectionFactory(protocol);
      Connection connection = factory.createConnection();
      connection.setClientID("validate");
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = session.createProducer(session.createTopic(ADDRESS.toString()));
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < 1000; i++) {
         builder.append('a' + (i %20));
      }

      connection.start();

      String text = builder.toString();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         TextMessage message = session.createTextMessage(text);
         producer.send(message);
         if (i > 0 && i % 1000 == 0) {
            session.commit();
            if (i == MESSAGE_COUNT / 2) {
               server0.destroyForcibly();
               Wait.assertTrue(activeMQServerControl::isActive);
            }
            try {
               System.out.println("Sent " + i);
               session.commit();
            } catch (Exception ignored) {
            }
         }
      }
      session.commit();

      Thread.sleep(15_000);

      // server0.destroyForcibly();
      // Wait.assertTrue(activeMQServerControl::isActive);

      System.out.println("Activated already");

      Thread.sleep(5_000);

      for (int i = 0; i < SLOW_THREADS + FAST_THREADS; i++) {
         String queueName = "consumerNR_" + i + ".nr_" + i;
         try {
            long messageCount = activeMQServerControl.getMessageCount(queueName);
            System.out.println("MessageCount:: on " + queueName + " is " + messageCount);
            Assert.assertTrue(messageCount >= 0);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      for (int i = 0; i < MESSAGE_COUNT / 2; i++) {
         TextMessage message = session.createTextMessage(text);
         producer.send(message);
         if (i > 0 && i % 1000 == 0) {
            session.commit();
            System.out.println("Sent " + i);
            session.commit();
         }
      }
      session.commit();

      Thread.sleep(1000);


      for (int i = 0; i < SLOW_THREADS + FAST_THREADS; i++) {
         String queueName = "consumerNR_" + i + ".nr_" + i;
         try {
            long messageCount = activeMQServerControl.getMessageCount(queueName);
            System.out.println("MessageCount:: on " + queueName + " is " + messageCount);
            Assert.assertTrue(messageCount >= 0);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      draining.set(true); // after I set this all consumers will be fast, and all messages should be consumed

      for (int i = 0; i < (SLOW_THREADS + FAST_THREADS); i++) {
         String queueName = "consumerNR_" + i + ".nr_" + i;
         // Fast queues should eventually remove all the messages
         Wait.assertTrue(queueName + " should be 0", () -> activeMQServerControl.getMessageCount(queueName) == 0, 120_000, 500);
      }

      running.set(false);


      threads.forEach((t) -> {
         try {
            t.join(5000);
         } catch (Exception e) {
         }
      });

   }

   protected MBeanServerConnection connectJMX(int port) throws Exception {
      // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
      String urlString = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + port + "/jmxrmi";

      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector jmxConnector = null;

      for (int retry = 0; retry < 20; retry++) {
         try {
            jmxConnector = JMXConnectorFactory.connect(url);
            addCloseable(jmxConnector);
            System.out.println("Successfully connected to: " + urlString);
         } catch (Exception e) {
            jmxConnector = null;
            e.printStackTrace();
            Thread.sleep(500);
         }
      }

      Assert.assertNotNull(jmxConnector);

      MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      return mBeanServerConnection;
   }

}
