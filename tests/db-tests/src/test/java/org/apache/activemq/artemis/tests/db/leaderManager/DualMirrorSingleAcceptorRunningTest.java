/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.activemq.artemis.tests.db.leaderManager;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.tests.db.common.DBTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test is dependent on zookeeper being provided before the tests start.
 *
 * To run this test against zookeeper you must enable the profile DB-ZK-tests
 * <ol>
 * <li>Start the ZK server using the script located at {@code tests/db-tests/scripts/start-ZK.sh}</li>
 * <li>Activate the DB-etc-tests and DB-ZK-tests Maven profiles:
 * <pre>
 * mvn test tests -P DB-ZK-tests
 * </pre>
 * </li>
 * </ol>
 */
public class DualMirrorSingleAcceptorRunningTest extends DBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_WITH_ZK_A = "brokerConnect/mirrorSingleAcceptor/ZK/A";
   public static final String SERVER_NAME_WITH_ZK_B = "brokerConnect/mirrorSingleAcceptor/ZK/B";

   public static final String SERVER_NAME_WITH_FILE_A = "brokerConnect/mirrorSingleAcceptor/file/A";
   public static final String SERVER_NAME_WITH_FILE_B = "brokerConnect/mirrorSingleAcceptor/file/B";

   // Test constants for testAlternatingServers()
   private static final int ALTERNATING_TEST_ITERATIONS = 4;
   private static final int MESSAGES_SENT_PER_ITERATION = 100;
   private static final int MESSAGES_CONSUMED_PER_ITERATION = 17;
   private static final int MESSAGES_REMAINING_PER_ITERATION = MESSAGES_SENT_PER_ITERATION - MESSAGES_CONSUMED_PER_ITERATION;
   private static final int EXPECTED_FINAL_MESSAGE_COUNT = ALTERNATING_TEST_ITERATIONS * MESSAGES_REMAINING_PER_ITERATION;

   Process processA;
   Process processB;

   @BeforeAll
   public static void createServers() throws Exception {
      {
         File fileLock = new File("./target/serverLock");
         fileLock.mkdirs();
         createServerPair(SERVER_NAME_WITH_FILE_A, SERVER_NAME_WITH_FILE_B,
                          "./src/main/resources/servers/mirrorSingleAcceptor/file/A",
                          "./src/main/resources/servers/mirrorSingleAcceptor/file/B",
                          s -> customizeFileServer(s, fileLock));
      }

      {
         createServerPair(SERVER_NAME_WITH_ZK_A, SERVER_NAME_WITH_ZK_B,
                          "./src/main/resources/servers/mirrorSingleAcceptor/ZK/A",
                          "./src/main/resources/servers/mirrorSingleAcceptor/ZK/B",
                          null);
      }
   }

   private static void customizeFileServer(File serverLocation, File fileLock) {
      try {
         FileUtil.findReplace(new File(serverLocation, "/etc/broker.xml"), "CHANGEME", fileLock.getAbsolutePath());
      } catch (Throwable e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   private static void createServerPair(String serverNameA, String serverNameB,
                                         String configPathA, String configPathB,
                                         Consumer<File> customizeServer) throws Exception {
      File serverLocationA = getFileServerLocation(serverNameA);
      File serverLocationB = getFileServerLocation(serverNameB);
      deleteDirectory(serverLocationB);
      deleteDirectory(serverLocationA);

      createSingleServer(serverLocationA, configPathA, "A", customizeServer);
      createSingleServer(serverLocationB, configPathB, "B", customizeServer);
   }

   private static void createSingleServer(File serverLocation, String configPath,
                                           String userAndPassword, Consumer<File> customizeServer) throws Exception {
      HelperCreate cliCreateServer = helperCreate();
      cliCreateServer.setAllowAnonymous(true)
                     .setUser(userAndPassword)
                     .setPassword(userAndPassword)
                     .setNoWeb(true)
                     .setConfiguration(configPath)
                     .setArtemisInstance(serverLocation);
      cliCreateServer.createServer();

      if (customizeServer != null) {
         customizeServer.accept(serverLocation);
      }
   }

   @BeforeEach
   public void prepareServers() throws Exception {
      cleanupData(SERVER_NAME_WITH_FILE_A);
      cleanupData(SERVER_NAME_WITH_FILE_B);

      cleanupData(SERVER_NAME_WITH_ZK_A);
      cleanupData(SERVER_NAME_WITH_ZK_B);
   }

   @EnabledIfSystemProperty(named = "ZK.load", matches = "true")
   @Test
   public void testAlternatingZK() throws Throwable {
      testAlternating(SERVER_NAME_WITH_ZK_A, SERVER_NAME_WITH_ZK_B);
   }

   @Test
   public void testAlternatingFile() throws Throwable {
      testAlternating(SERVER_NAME_WITH_FILE_A, SERVER_NAME_WITH_FILE_B);
   }

   public void testAlternating(String nameServerA, String nameServerB) throws Throwable {
      processA = startServer(nameServerA, 0, -1);
      waitForXToStart();
      processB = startServer(nameServerB, 0, -1);
      ConnectionFactory cfX = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61616");

      for (int i = 0; i < ALTERNATING_TEST_ITERATIONS; i++) {
         logger.info("Iteration {}: Server {} active", i, (i % 2 == 0) ? "A" : "B");

         if (i % 2 == 0) {
            // Even iteration: Server A active, kill Server B
            killServer(processB);
            waitForXToStart();
         } else {
            // Odd iteration: Server B active, kill Server A
            killServer(processA);
            waitForXToStart();
         }

         // Send messages through the shared acceptor
         cfX = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61616");
         sendMessages(cfX, MESSAGES_SENT_PER_ITERATION);

         // Consume some messages
         receiveMessages(cfX, MESSAGES_CONSUMED_PER_ITERATION);

         // Restart the killed server
         if (i % 2 == 0) {
            processB = startServer(nameServerB, 0, -1);
         } else {
            processA = startServer(nameServerA, 0, -1);
         }
      }

      // Verify they both have the expected message count (iterations × (sent - consumed))
      assertMessageCount("tcp://localhost:61000", "myQueue", EXPECTED_FINAL_MESSAGE_COUNT);
      assertMessageCount("tcp://localhost:61001", "myQueue", EXPECTED_FINAL_MESSAGE_COUNT);
   }

   private static void sendMessages(ConnectionFactory cfX, int nmessages) throws JMSException {
      try (Connection connectionX = cfX.createConnection("A", "A")) {
         Session sessionX = connectionX.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = sessionX.createQueue("myQueue");
         MessageProducer producerX = sessionX.createProducer(queue);
         for (int i = 0; i < nmessages; i++) {
            producerX.send(sessionX.createTextMessage("hello " + i));
         }
         sessionX.commit();
      }
   }

   private static void receiveMessages(ConnectionFactory cfX, int nmessages) throws JMSException {
      try (Connection connectionX = cfX.createConnection("A", "A")) {
         connectionX.start();
         Session sessionX = connectionX.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = sessionX.createQueue("myQueue");
         MessageConsumer consumerX = sessionX.createConsumer(queue);
         for (int i = 0; i < nmessages; i++) {
            TextMessage message = (TextMessage) consumerX.receive(5000);
            assertNotNull(message, "Expected message " + i + " but got null");
         }
         sessionX.commit();
      }
   }

   private void waitForXToStart() {
      for (int i = 0; i < 20; i++) {
         try {
            ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:61616");
            Connection connection = factory.createConnection();
            connection.close();
            return;
         } catch (Throwable e) {
            logger.debug(e.getMessage(), e);
            try {
               Thread.sleep(500);
            } catch (Throwable ignored) {
            }
         }
      }
   }

   protected void assertMessageCount(String uri, String queueName, int count) throws Exception {
      SimpleManagement simpleManagement = new SimpleManagement(uri, null, null);
      Wait.assertEquals(count, () -> {
         try {
            return simpleManagement.getMessageCountOnQueue(queueName);
         } catch (Throwable e) {
            return -1;
         }
      });
   }

}