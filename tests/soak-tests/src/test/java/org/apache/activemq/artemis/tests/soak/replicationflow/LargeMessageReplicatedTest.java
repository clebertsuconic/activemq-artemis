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
package org.apache.activemq.artemis.tests.soak.replicationflow;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LargeMessageReplicatedTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_0 = "large-server0";
   public static final String SERVER_NAME_1 = "large-server0-replica";
   public static final String SERVER_NAME_2 = "large-server1";

   @BeforeAll
   public static void createServers() throws Exception {
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_0);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
         cliCreateServer.setConfiguration("./src/main/resources/servers/replica-large/server0");
         cliCreateServer.createServer();
      }
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_1);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
         cliCreateServer.setConfiguration("./src/main/resources/servers/replica-large/server0-replica");
         cliCreateServer.createServer();
      }
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_2);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
         cliCreateServer.setConfiguration("./src/main/resources/servers/replica-large/server1");
         cliCreateServer.createServer();
      }
   }
   private static Process server0;
   private static Process server1;
   private static Process server2;

   @BeforeEach
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      disableCheckThread();
   }

   @AfterEach
   @Override
   public void after() throws Exception {
      super.after();
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
   }

   @Test
   public void testSendLargeStop() throws Exception {
      internalTest("exampleQueue", "tcp://localhost:61617", false);
   }

   @Test
   public void testSendLargeStopOverBridge() throws Exception {
      internalTest("bridgeQueue", "tcp://localhost:61618", true);
   }

   private void internalTest(final String destination, final String consumerURI, boolean consumeFromServer2) throws Exception {


      final int SENDER_PAIRS = 30;
      ExecutorService executorService = Executors.newFixedThreadPool(SENDER_PAIRS * 2);
      runAfter(executorService::shutdownNow);

      AtomicInteger messagesSent = new AtomicInteger(0);
      AtomicBoolean running = new AtomicBoolean(true);
      runAfter(() -> running.set(false));

      String messageBody;

      {
         StringBuilder builder = new StringBuilder();
         while (builder.length() < 10 * 1024 * 1024) {
            builder.append("This is a large body ");
         }
         messageBody = builder.toString();
      }

      server0 = startServer(SERVER_NAME_0, 0, 30000);
      server1 = startServer(SERVER_NAME_1, 0, 30000);

      try (SimpleManagement simpleManagement0 = new SimpleManagement("tcp://localhost:61616", "a", "a")) {
         Wait.assertTrue(simpleManagement0::isReplicaSync, 5000, 100);
      }

      CountDownLatch latchReady = new CountDownLatch(SENDER_PAIRS * 4);
      for (int i = 0; i < SENDER_PAIRS; i++) {
         executorService.execute(() -> {
            try {
               ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
               try (Connection connection = factory.createConnection()) {
                  Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                  Queue queue = session.createQueue(destination);
                  MessageProducer producer = session.createProducer(queue);
                  while (running.get()) {
                     producer.send(session.createTextMessage(messageBody));
                     session.commit();
                     messagesSent.incrementAndGet();
                     latchReady.countDown();
                  }
               }

            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
            }
         });

         executorService.execute(() -> {
            try {
               XAConnectionFactory factory = (XAConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
               try (XAConnection connection = factory.createXAConnection()) {
                  XASession session = connection.createXASession();
                  Queue queue = session.createQueue(destination);
                  MessageProducer producer = session.createProducer(queue);
                  while (running.get()) {
                     XidImpl xid = newXID();
                     session.getXAResource().start(xid, XAResource.XA_OK);
                     producer.send(session.createTextMessage(messageBody));
                     session.getXAResource().end(xid, XAResource.TMSUCCESS);
                     session.getXAResource().prepare(xid);
                     session.getXAResource().commit(xid, false);
                     messagesSent.incrementAndGet();
                     latchReady.countDown();
                  }
               }

            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
            }
         });


      }

      assertTrue(latchReady.await(100, TimeUnit.SECONDS));

      SimpleManagement simpleManagementConsumer = new SimpleManagement(consumerURI, null, null);
      SimpleManagement simpleManagementServer1_backup = new SimpleManagement("tcp://localhost:61617", null, null);

      if (consumeFromServer2) {
         server2 = startServer(SERVER_NAME_2, 2, 30000);
         Wait.assertTrue(() -> getCountAndNoException(simpleManagementConsumer, destination) > 0, 5000, 100);
      }

      stopServerWithFile(getServerLocation(SERVER_NAME_0));

      // this one is just to make sure server1 was activated
      Wait.assertTrue(() -> getCountAndNoException(simpleManagementServer1_backup, destination) >= 0, 15_000, 500);

      fakeXARecovery();

      Wait.assertTrue(() -> getCountAndNoException(simpleManagementConsumer, destination) >= messagesSent.get(), 15_000, 500);

      int messageCount = (int)simpleManagementConsumer.getMessageCountOnQueue(destination);

      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", consumerURI);
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         connection.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(destination));
         for (int i = 0; i < messageCount; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertNotNull(message);
            assertEquals(messageBody, message.getText());
         }
      }

   }

   // it will scan any pending prepares and commit them
   private void fakeXARecovery() throws Exception {
      XAConnectionFactory factory = (XAConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61617");
      try (XAConnection connection = factory.createXAConnection()) {
         XASession session = connection.createXASession();
         Xid[] recoveredXids = session.getXAResource().recover(XAResource.TMSTARTRSCAN);
         if (recoveredXids != null && recoveredXids.length != 0) {
            for (Xid xid : recoveredXids) {
               logger.info("prepared XA!!!!");
               session.getXAResource().commit(xid, false);
            }
         }
         assertEquals(0, session.getXAResource().recover(XAResource.TMENDRSCAN).length);
      }
   }

   private long getCountAndNoException(SimpleManagement simpleManagement, String destination) {
      try {
         long result = simpleManagement.getMessageAddedOnQueue(destination);
         logger.info("Returning {}", result, new Exception("trace"));
         return result;
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         return -1l;
      }

   }

}
