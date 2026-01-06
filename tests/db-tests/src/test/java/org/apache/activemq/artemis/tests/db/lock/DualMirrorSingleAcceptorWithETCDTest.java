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
package org.apache.activemq.artemis.tests.db.lock;

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

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.tests.db.common.DBTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test is disabled by default and only runs when the etcd.load system property is set to true.
 * To enable this test:
 * <ol>
 * <li>Start the etcd server using the script located at {@code tests/db-tests/scripts/start-etcd.sh}</li>
 * <li>Activate the DB-etc-tests Maven profile:
 * <pre>
 * mvn test -P DB-etc-tests
 * </pre>
 * </li>
 * </ol>
 */
@EnabledIfSystemProperty(named = "etcd.load", matches = "true")
public class DualMirrorSingleAcceptorWithETCDTest extends DBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_A = "brokerConnect/mirrorSingleAcceptor/A";
   public static final String SERVER_NAME_B = "brokerConnect/mirrorSingleAcceptor/B";

   // Test constants for testAlternatingServers()
   private static final int ALTERNATING_TEST_ITERATIONS = 10;
   private static final int MESSAGES_SENT_PER_ITERATION = 100;
   private static final int MESSAGES_CONSUMED_PER_ITERATION = 17;
   private static final int MESSAGES_REMAINING_PER_ITERATION = MESSAGES_SENT_PER_ITERATION - MESSAGES_CONSUMED_PER_ITERATION;
   private static final int EXPECTED_FINAL_MESSAGE_COUNT = ALTERNATING_TEST_ITERATIONS * MESSAGES_REMAINING_PER_ITERATION;

   Process processA;
   Process processB;

   @BeforeAll
   public static void createServers() throws Exception {

      File server0Location = getFileServerLocation(SERVER_NAME_A);
      File server1Location = getFileServerLocation(SERVER_NAME_B);
      deleteDirectory(server1Location);
      deleteDirectory(server0Location);

      {
         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setUser("A").setPassword("A").setNoWeb(true).setConfiguration("./src/main/resources/servers/mirrorSingleAcceptor/A").setArtemisInstance(server0Location);
         cliCreateServer.createServer();
      }

      {
         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setUser("B").setPassword("B").setNoWeb(true).setConfiguration("./src/main/resources/servers/mirrorSingleAcceptor/B").setArtemisInstance(server1Location);
         cliCreateServer.createServer();
      }
   }

   @BeforeEach
   public void prepareServers() throws Exception {
      cleanupData(SERVER_NAME_A);
      cleanupData(SERVER_NAME_B);
      processA = startServer(SERVER_NAME_A, 0, -1);
      waitForXToStart();
      processB = startServer(SERVER_NAME_B, 0, -1);
   }

   @Test
   public void testAlternatingServers() throws Throwable {
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
            processB = startServer(SERVER_NAME_B, 0, -1);
         } else {
            processA = startServer(SERVER_NAME_A, 0, -1);
         }
      }

      // Both servers are running after loop (iteration 9 restarts A, B was already running)
      // Verify they both have the expected message count (iterations × (sent - consumed))
      assertMessageCount("tcp://localhost:61000", "someQueue", EXPECTED_FINAL_MESSAGE_COUNT);
      assertMessageCount("tcp://localhost:61001", "someQueue", EXPECTED_FINAL_MESSAGE_COUNT);
   }

   private static void sendMessages(ConnectionFactory cfX, int nmessages) throws JMSException {
      try (Connection connectionX = cfX.createConnection("A", "A")) {
         // Testing things on the direction from mirroring from A to B...
         Session sessionX = connectionX.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = sessionX.createQueue("someQueue");
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
         Queue queue = sessionX.createQueue("someQueue");
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
            logger.warn(e.getMessage(), e);
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