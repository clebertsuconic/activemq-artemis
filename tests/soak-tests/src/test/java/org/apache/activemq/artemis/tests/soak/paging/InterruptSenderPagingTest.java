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
package org.apache.activemq.artemis.tests.soak.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class InterruptSenderPagingTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_0 = "interrupt-sender";

   @BeforeAll
   public static void createServers() throws Exception {
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_0);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
         cliCreateServer.setConfiguration("./src/main/resources/servers/interrupt-sender");
         cliCreateServer.createServer();
      }
   }
   private static Process server0;

   @BeforeEach
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      disableCheckThread();
   }

   @AfterEach
   @Override
   public void after() throws Exception {
      super.after();
   }

   @Test
   public void testInterruptNoMessageLoss() throws Exception {
      final int SENDER_PAIRS = 30;
      ExecutorService executorService = Executors.newFixedThreadPool(SENDER_PAIRS * 2);
      runAfter(executorService::shutdownNow);

      AtomicBoolean running = new AtomicBoolean(true);
      runAfter(() -> running.set(false));

      String messageBody;

      {
         StringBuilder builder = new StringBuilder();
         while (builder.length() < 200 * 1024) {
            builder.append("This is a large body ");
         }
         messageBody = builder.toString();
      }

      server0 = startServer(SERVER_NAME_0, 0, 30000);


      AtomicInteger sequenceGenerator = new AtomicInteger(1);

      ConcurrentHashSet<String> list = new ConcurrentHashSet<>();

      CountDownLatch latchReady = new CountDownLatch(SENDER_PAIRS * 4);
      CountDownLatch latchDone = new CountDownLatch(SENDER_PAIRS);
      for (int i = 0; i < SENDER_PAIRS; i++) {
         executorService.execute(() -> {
            try {
               ConnectionFactory factory = null;
               Connection connection = null;
               Session session = null;
               MessageProducer producer = null;
               String dupID = String.valueOf(sequenceGenerator.incrementAndGet());
               while (running.get()) {
                  try {
                     if (factory == null) {
                        factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
                        connection = factory.createConnection();
                        session = connection.createSession(true, Session.SESSION_TRANSACTED);
                        producer = session.createProducer(session.createQueue("exampleQueue"));
                     }
                     TextMessage message = session.createTextMessage(messageBody);
                     message.setStringProperty("myID", dupID);
                     message.setStringProperty("_AMQ_DUPL_ID", dupID);
                     logger.info("sending dupID={}", dupID);
                     producer.send(message);
                     session.commit();
                     list.add(dupID);
                     logger.info("sending OK dupID={}", dupID);
                     dupID = String.valueOf(sequenceGenerator.incrementAndGet());
                     latchReady.countDown();
                  } catch (Exception e) {
                     if (e instanceof JMSException && e.getMessage().contains("Duplicate message detected")) {
                        logger.info("duplicateID fine dupID={} error={}", dupID, e.getMessage());
                        list.add(dupID);
                        dupID = String.valueOf(sequenceGenerator.incrementAndGet());
                     } else {
                        logger.warn("error on dupID={}, error Message={}", dupID, e.getMessage(), e);
                        factory = null;
                        connection = null;
                        session = null;
                        producer = null;
                     }
                  }
               }

               try {
                  connection.close();
               } catch (Throwable ignored) {
               }
            } finally {
               latchDone.countDown();
            }
         });
      }

      assertTrue(latchReady.await(100, TimeUnit.SECONDS));
      server0.destroyForcibly();
      assertTrue(server0.waitFor(10, TimeUnit.SECONDS));

      server0 = startServer(SERVER_NAME_0, 0, 60_000);

      Thread.sleep(1_000);
      running.set(false);
      assertTrue(latchDone.await(10, TimeUnit.SECONDS));

      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         connection.start();
         QueueBrowser consumer = session.createBrowser(session.createQueue("exampleQueue"));
         Enumeration messageEnumeration = consumer.getEnumeration();
         while (!list.isEmpty()) {
            if (!messageEnumeration.hasMoreElements()) {
               break;
            }
            TextMessage message = (TextMessage) messageEnumeration.nextElement();
            String myid = message.getStringProperty("myID");
            logger.info("Received dupID={}", myid);
            assertNotNull(message);
            if (!list.remove(myid)) {
               logger.info("Could not find {}", myid);
               fail("Could not find " + myid);
            }
         }
         if (!list.isEmpty()) {
            logger.info("Messages that were still in the list");
            for (String missedDuplicate : list) {
               logger.info("Missed duplicate dupID={}", missedDuplicate);
            }
         }
         assertTrue(list.isEmpty());
      }

   }

}
