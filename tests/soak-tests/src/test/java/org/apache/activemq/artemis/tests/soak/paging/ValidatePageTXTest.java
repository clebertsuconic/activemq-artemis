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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
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

public class ValidatePageTXTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_0 = "validate-page-tx";

   @BeforeAll
   public static void createServers() throws Exception {
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_0);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
         cliCreateServer.setConfiguration("./src/main/resources/servers/validate-page-tx");
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
   public void testValidateTXServerStop() throws Exception {
      final String coreURI = "tcp://localhost:61616?callTimeout=1000";
      final String amqpURI = "tcp://localhost:61616";
      final int SEND_GROUPS = 5;
      final int GROUPS = 3; // XA Core, Regular TX Core, AMQP TX
      final int BODY_SIZE = 200 * 1024;
      AtomicBoolean running = new AtomicBoolean(true);
      ExecutorService executorService = Executors.newFixedThreadPool(SEND_GROUPS * GROUPS);
      runAfter(executorService::shutdownNow);

      runAfter(() -> running.set(false));

      String largeBody;

      {
         StringBuilder builder = new StringBuilder();
         while (builder.length() < BODY_SIZE) {
            builder.append("This is a large body ");
         }
         largeBody = builder.toString();
      }

      server0 = startServer(SERVER_NAME_0, 0, 30000);

      AtomicInteger sequenceGenerator = new AtomicInteger(1);

      ConcurrentHashSet<String> dupList = new ConcurrentHashSet<>();

      CountDownLatch enoughSent = new CountDownLatch(SEND_GROUPS * GROUPS * 2);
      CountDownLatch latchDone = new CountDownLatch(SEND_GROUPS * GROUPS);
      AtomicBoolean waitAfterError = new AtomicBoolean(true);

      CyclicBarrier startFlag = new CyclicBarrier(SEND_GROUPS * GROUPS + 1);
      CyclicBarrier errorCaptureFlag = new CyclicBarrier(SEND_GROUPS * GROUPS + 1);

      int threadCounts = 0;

      // instead of running this test once for XA and once for NON XA
      // I start half threads with XA commits, half threads with regular commits
      for (int i = 0; i < SEND_GROUPS; i++) {
         String threadID = "CoreThread" + (threadCounts++);
         executorService.execute(() -> {
            sender(false, "core", coreURI, threadID, startFlag, errorCaptureFlag, waitAfterError, sequenceGenerator, running, largeBody, dupList, enoughSent, latchDone);
         });
      }
      for (int i = 0; i < SEND_GROUPS; i++) {
         String threadID = "XAThread" + (threadCounts++);
         executorService.execute(() -> {
            sender(true, "core", coreURI, threadID, startFlag, errorCaptureFlag, waitAfterError, sequenceGenerator, running, largeBody, dupList, enoughSent, latchDone);
         });
      }
      for (int i = 0; i < SEND_GROUPS; i++) {
         String threadID = "AMQPThread" + (threadCounts++);
         executorService.execute(() -> {
            sender(false, "AMQP", amqpURI, threadID, startFlag, errorCaptureFlag, waitAfterError, sequenceGenerator, running, largeBody, dupList, enoughSent, latchDone);
         });
      }

      logger.info("Start flag waiting");
      startFlag.await(30, TimeUnit.SECONDS);

      assertTrue(enoughSent.await(100, TimeUnit.SECONDS));
      logger.info("Enough messages sent, killing server");


      for (int errorLoop = 0; errorLoop < 2; errorLoop++) {
         if (errorLoop == 0) {
            server0.destroyForcibly();
         } else {
            stopServerWithFile(getServerLocation(SERVER_NAME_0));
         }
         assertTrue(server0.waitFor(10, TimeUnit.SECONDS));
         logger.info("Waiting aligned flags for error part...");
         // everybody should be aligned in the barrier after the error
         // this is because I want to scan for prepared transactions and commit them
         errorCaptureFlag.await(30, TimeUnit.SECONDS);

         logger.info("restarting server...");
         server0 = startServer(SERVER_NAME_0, 0, 60_000);
         logger.info("Started servers, doing recovery");
         fakeXARecovery();
         logger.info("Releasing countdown after error");

         if (errorLoop == 1) {
            waitAfterError.set(false);
         }

         // aligning again to restart
         startFlag.await(30, TimeUnit.SECONDS);

      }

      logger.info("Waiting some time");
      Thread.sleep(100);
      running.set(false);

      logger.info("running set to false");
      assertTrue(latchDone.await(300, TimeUnit.SECONDS));
      logger.info("latch done is done.. shutting down executor");

      executorService.shutdownNow();
      assertTrue(executorService.awaitTermination(60, TimeUnit.SECONDS));

      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         connection.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue("exampleQueue"));
         while (!dupList.isEmpty()) {
            TextMessage message = (TextMessage) consumer.receive(10_000);
            if (message == null) {
               break;
            }
            String myid = message.getStringProperty("myID");
            logger.info("Received dupID={}", myid);
            assertNotNull(message);
            assertEquals(largeBody, message.getText());
            if (!dupList.remove(myid)) {
               logger.info("Could not find {}", myid);
               fail("Could not find " + myid);
            }
         }
         session.rollback();
         if (!dupList.isEmpty()) {
            logger.info("Messages that were still in the dupList");
            for (String missedDuplicate : dupList) {
               logger.info("Missed duplicate dupID={}", missedDuplicate);
            }
         }
         assertTrue(dupList.isEmpty());
      }

   }

   // it will scan any pending prepares and commit them
   private void fakeXARecovery() throws Exception {
      XAConnectionFactory factory = (XAConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
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

   private void sender(boolean useXA,
                       String protocol,
                       String uri,
                       String threadID,
                       CyclicBarrier startFlag,
                       CyclicBarrier errorCaptureFlag,
                       AtomicBoolean waitAfterError,
                       AtomicInteger sequenceGenerator,
                       AtomicBoolean running,
                       String messageBody,
                       ConcurrentHashSet<String> dupList,
                       CountDownLatch enoughSent,
                       CountDownLatch latchDone) {
      try {
         ConnectionFactory factory = null;
         XAConnectionFactory xaConnectionFactory = null;
         Connection connection = null;
         XAConnection xaConnection = null;
         Session session = null;
         XASession xasession = null;
         MessageProducer producer = null;
         String dupID = String.valueOf(sequenceGenerator.incrementAndGet());
         boolean firstTime = true;
         while (running.get()) {
            try {
               if (factory == null) {
                  if (useXA) {
                     xaConnectionFactory = (XAConnectionFactory) CFUtil.createConnectionFactory(protocol, uri);
                     xaConnection = xaConnectionFactory.createXAConnection();
                     connection = xaConnection;
                     xasession = xaConnection.createXASession();
                     session = xasession;
                  } else {
                     factory = CFUtil.createConnectionFactory("CORE", uri);
                     connection = factory.createConnection();
                     session = connection.createSession(true, Session.SESSION_TRANSACTED);
                  }
                  producer = session.createProducer(session.createQueue("exampleQueue"));
               }
               Xid xid = null;
               if (useXA) {
                  xid = newXID();
                  xasession.getXAResource().start(xid, XAResource.XA_OK);
               }
               TextMessage message = session.createTextMessage(messageBody);
               message.setStringProperty("myID", dupID);
               message.setStringProperty("_AMQ_DUPL_ID", dupID);
               logger.info("sending dupID={}, threadID={}", dupID, threadID);
               if (firstTime) {
                  firstTime = false;
                  logger.info("Thread {} waiting a start flag", threadID);
                  startFlag.await(10, TimeUnit.SECONDS);
                  logger.info("Thread {} received the start flag", threadID);
               }
               producer.send(message);
               if (useXA) {
                  xasession.getXAResource().end(xid, XAResource.TMSUCCESS);
                  xasession.getXAResource().prepare(xid);
                  dupList.add(dupID);
                  xasession.getXAResource().commit(xid, false);

               } else {
                  session.commit();
                  dupList.add(dupID);
               }
               logger.info("sending OK dupID={}, threadID={}", dupID, threadID);
               dupID = String.valueOf(sequenceGenerator.incrementAndGet());
               enoughSent.countDown();
            } catch (Throwable e) {
               logger.info("error at thread {}, message={}", threadID, e.getMessage(), e);
               if (waitAfterError.get()) {
                  try {
                     // align once before XA recovery
                     logger.info("thread {} waiting for startFlag first time", threadID);
                     errorCaptureFlag.await(30, TimeUnit.SECONDS);
                     // we align again to restart
                     startFlag.await(30, TimeUnit.SECONDS);
                  } catch (Throwable e2) {
                     logger.warn("Exception of the exception on {}, message={}", threadID, e2.getMessage(), e2);
                  }
               }
               if (e.getCause() != null && e.getCause() instanceof ActiveMQDuplicateIdException) {
                  logger.info("duplicateID fine dupID={} error={}, threadID={}", dupID, e.getMessage(), threadID);
                  dupList.add(dupID);
                  dupID = String.valueOf(sequenceGenerator.incrementAndGet());
               } else {
                  if (e.getCause() instanceof ActiveMQDuplicateIdException) {
                     new Exception("WHAAT!!! the type is " + e.getClass()).printStackTrace();
                     System.exit(-1);
                  }

                  logger.warn("error on dupID={}, error Message={}", dupID, e.getMessage(), e);
                  factory = null;
                  connection = null;
                  xasession = null;
                  xaConnectionFactory = null;
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
   }

}
