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
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultipleProducersTest extends SmokeTestBase {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_0 = "multiple-producers-paging";

   @BeforeAll
   public static void createServers() throws Exception {

      File server0Location = getFileServerLocation(SERVER_NAME_0);
      deleteDirectory(server0Location);

      {
         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setUser("admin").setPassword("admin").setAllowAnonymous(true).setNoWeb(false).setArtemisInstance(server0Location).setConfiguration("./src/main/resources/servers/multipleProducersPaging");
         cliCreateServer.createServer();
      }
   }

   @BeforeEach
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      startServer(SERVER_NAME_0, 0, 30000);
   }


   @Test
   public void testMultipleProducers() throws Exception {
      String protocol = "amqp";
      int producers = 100;
      int consumers = 1;
      int messagesPerProducer = 100;
      int totalMessages = producers * messagesPerProducer;
      assertTrue(totalMessages % consumers == 0, "totalMessages % consumers must be 0");
      int messagesPerConsumer = totalMessages / consumers;

      String queueName = "MultipleProducers";
      AtomicInteger errors = new AtomicInteger();

      CountDownLatch done = new CountDownLatch(producers + consumers);
      AtomicInteger distance = new AtomicInteger(0);

      final AtomicInteger[] messagesSent = new AtomicInteger[producers];
      for (int i = 0; i < messagesSent.length; i++) {
         messagesSent[i] = new AtomicInteger(0);
      }

      final AtomicInteger[] messagesConsumed = new AtomicInteger[consumers];
      for (int i = 0; i < messagesConsumed.length; i++) {
         messagesConsumed[i] = new AtomicInteger(0);
      }

      ExecutorService executor = Executors.newFixedThreadPool(producers + consumers + 1);
      runAfter(executor::shutdownNow);

      AtomicBoolean running = new AtomicBoolean(true);

      executor.execute(() -> {
         try {
            while (running.get()) {
               done.await(1, TimeUnit.SECONDS);
               StringBuilder builder = new StringBuilder();
               int produced = 0, consumed = 0;
               for (int i = 0; i < producers; i++) {
                  builder.append("Producer[" + i + "] sent " + messagesSent[i] + "\n");
                  produced += messagesSent[i].get();
               }
               for (int i = 0; i < consumers; i++) {
                  builder.append("Consumer[" + i + "] received " + messagesConsumed[i] + "\n");
                  consumed += messagesConsumed[i].get();
               }
               builder.append("Total produced: " + produced + "\n");
               builder.append("Total consumed: " + consumed + "\n");
               logger.info("\n{}", builder.toString());
            }
         } catch (InterruptedException expected) {
         }
      });

      for (int i = 0; i < producers; i++) {
         int producerID = i;

         executor.execute(() -> {
            ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
            try (Connection connection = factory.createConnection()) {
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               Queue queue = session.createQueue(queueName);
               MessageProducer producer = session.createProducer(queue);
               for (int produced = 0; produced < messagesPerProducer; produced++) {
                  producer.send(session.createTextMessage("hello hello"));
                  messagesSent[producerID].incrementAndGet();
                  distance.incrementAndGet();
               }
            } catch (Exception e) {
               errors.incrementAndGet();
               logger.warn(e.getMessage(), e);
            } finally {
               done.countDown();
            }
         });
      }
      for (int i = 0; i < consumers; i++) {
         int consumerID = i;

         executor.execute(() -> {
            ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
            try (Connection connection = factory.createConnection()) {
               connection.start();
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               Queue queue = session.createQueue(queueName);
               MessageConsumer consumer = session.createConsumer(queue);
               for (int consumed = 0; consumed < messagesPerConsumer; consumed++) {
                  if (distance.get() < 2) {
                     Thread.sleep(500);
                  }
                  Message message = consumer.receive(10_000);
                  assertNotNull(message);
                  messagesConsumed[consumerID].incrementAndGet();
                  distance.decrementAndGet();
               }
            } catch (Throwable e) {
               errors.incrementAndGet();
               logger.warn(e.getMessage(), e);
            } finally {
               done.countDown();
            }
         });
      }

      assertTrue(done.await(10, TimeUnit.MINUTES));
      assertEquals(0, errors.get());

   }


}
