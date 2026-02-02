/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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
package org.apache.activemq.artemis.tests.smoke.amqplargemessage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for ENTMQBR-7300: IllegalArgumentException when closing AMQP consumer during large message streaming.
 * This test reproduces the scenario where a ProtonServerSenderContext attempts to send data through a closed
 * sender during large message delivery.
 */
public class AMQPLargeMessageStreamTest extends SmokeTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME = "amqp-large-message";
   private static final int MESSAGE_COUNT = 100;
   private static final int MESSAGE_SIZE = 1500000; // 1.5 MB
   private static final int THREADS = 15;

   @BeforeAll
   public static void createServer() throws Exception {
      File serverLocation = getFileServerLocation(SERVER_NAME);
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = helperCreate();
      cliCreateServer.setUser("admin")
                     .setPassword("admin")
                     .setAllowAnonymous(true)
                     .setNoWeb(true)
                     .setArtemisInstance(serverLocation);
      cliCreateServer.createServer();

      // Configure multicast topic in broker.xml
      File brokerXml = new File(serverLocation, "etc/broker.xml");

      // Add multicast topic configuration
      String addressConfig = "         <address name=\"testTopic\">\n" +
                           "            <multicast/>\n" +
                           "         </address>\n" +
                           "      </addresses>";

      FileUtil.findReplace(brokerXml, "      </addresses>", addressConfig);
   }

   @BeforeEach
   public void before() throws Exception {
      cleanupData(SERVER_NAME);
      disableCheckThread();
      startServer(SERVER_NAME, 0, 30000);
   }

   @Test
   public void testAMQPLargeMessageStreaming() throws Exception {
      File serverLocation = getFileServerLocation(SERVER_NAME);

      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");

      final CyclicBarrier consumersReady = new CyclicBarrier(THREADS + 1);
      final CountDownLatch consumersDone = new CountDownLatch(THREADS);
      final CountDownLatch producersDone = new CountDownLatch(THREADS);
      final AtomicInteger receivedCount = new AtomicInteger(0);
      final AtomicInteger sentCount = new AtomicInteger(0);
      final AtomicInteger countError = new AtomicInteger(0);

      ExecutorService consumerExecutor = Executors.newFixedThreadPool(THREADS);
      runAfter(consumerExecutor::shutdownNow);
      ExecutorService producerExecutor = Executors.newFixedThreadPool(THREADS);
      runAfter(producerExecutor::shutdownNow);

      try {
         // Start consumers with durable subscriptions
         logger.info("Starting {} consumer threads...", THREADS);
         for (int i = 0; i < THREADS; i++) {
            final int threadId = i;
            consumerExecutor.execute(() -> {
               try {
                  consume(factory, threadId, consumersReady);
               } catch (Throwable error) {
                  countError.incrementAndGet();
                  logger.warn(error.getMessage(), error);
               } finally {
                  consumersDone.countDown();
               }
            });
         }

         // Wait for consumers to be ready
         logger.info("Waiting for consumers to be ready...");
         consumersReady.await(30, TimeUnit.SECONDS);

         // Start producers
         logger.info("Starting {} producer threads...", THREADS);
         for (int i = 0; i < THREADS; i++) {
            final int threadId = i;
            producerExecutor.execute(() -> {
               try {
                  produce(factory, sentCount, threadId);
               } catch (Throwable e) {
                  countError.incrementAndGet();
                  logger.warn(e.getMessage(), e);
               } finally {
                  producersDone.countDown();
               }
            });
         }

         // Wait for all messages to be received
         logger.info("Waiting for all messages to be received...");
         assertTrue(consumersDone.await(5, TimeUnit.MINUTES));
         assertTrue(producersDone.await(5, TimeUnit.MINUTES));

         logger.info("Test completed. Sent: {}, Received: {}", sentCount.get(), receivedCount.get());

      } finally {
         producerExecutor.shutdownNow();
         consumerExecutor.shutdownNow();
      }

      // Check server logs for IllegalArgumentException
      File logFile = new File(serverLocation, "log/artemis.log");
      boolean hasIllegalArgumentException = findLogRecord(logFile, "IllegalArgumentException");

      // Assert that no IllegalArgumentException was thrown
      assertFalse(hasIllegalArgumentException,
                  "Server log contains IllegalArgumentException - the bug is still present");
   }

   private static void produce(ConnectionFactory factory, AtomicInteger sentCount, int threadId) throws Throwable {
      Connection connection = null;
      try {
         connection = factory.createConnection();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic("testTopic");
         MessageProducer producer = session.createProducer(topic);

         int messagesPerThread = MESSAGE_COUNT / THREADS;
         int commitInterval = 10;

         byte[] payload = new byte[MESSAGE_SIZE];
         for (int j = 0; j < MESSAGE_SIZE; j++) {
            payload[j] = (byte) (j % 256);
         }

         for (int j = 0; j < messagesPerThread; j++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            producer.send(message);
            sentCount.incrementAndGet();

            logger.info("Sending {} on thread", j, threadId);

            if ((j + 1) % commitInterval == 0) {
               session.commit();
            }
         }
         session.commit();

         logger.info("Producer thread {} sent {} messages", threadId, messagesPerThread);
      } finally {
         if (connection != null) {
            try {
               connection.close();
            } catch (Exception e) {
               logger.error("Error closing producer connection", e);
            }
         }
      }
   }

   private static void consume(ConnectionFactory factory,
                                 int threadId,
                                 CyclicBarrier startFlag) throws Exception {
      Connection connection = null;
      try {
         connection = factory.createConnection();
         connection.setClientID("consumer-" + threadId);

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic("testTopic");

         // Create durable subscriber
         MessageConsumer consumer = session.createDurableSubscriber(topic, "sub-" + threadId);

         startFlag.await(10, TimeUnit.SECONDS);
         connection.start();

         int messagesPerThread = MESSAGE_COUNT / THREADS;
         int commitInterval = 100;
         int receivedInThread = 0;

         while (receivedInThread < messagesPerThread) {
            Message msg = consumer.receive(TimeUnit.SECONDS.toMillis(30));
            if (msg != null) {
               receivedInThread++;

               if (receivedInThread % commitInterval == 0) {
                  session.commit();
               }
            }
         }
         session.commit();

         logger.info("Consumer thread {} received {} messages", threadId, receivedInThread);
      } finally {
         if (connection != null) {
            try {
               connection.close();
            } catch (Exception e) {
               logger.error("Error closing consumer connection", e);
            }
         }
      }
   }
}
