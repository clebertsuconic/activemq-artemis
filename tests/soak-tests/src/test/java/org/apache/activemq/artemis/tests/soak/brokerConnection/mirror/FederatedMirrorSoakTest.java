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

package org.apache.activemq.artemis.tests.soak.brokerConnection.mirror;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FederatedMirrorSoakTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static String largeBody;
   private static String smallBody = "This is a small body";

   static {
      StringWriter writer = new StringWriter();
      while (writer.getBuffer().length() < 1024 * 1024) {
         writer.append("This is a large string ..... ");
      }
      largeBody = writer.toString();
   }

   public static final String DC1_NODE_A = "FederatedMirror/DC1/A";
   public static final String DC2_NODE_A = "FederatedMirror/DC2/A";
   public static final String DC1_NODE_B = "FederatedMirror/DC1/B";
   public static final String DC2_NODE_B = "FederatedMirror/DC2/B";

   Process processDC1_node_A;
   Process processDC1_node_B;
   Process processDC2_node_A;
   Process processDC2_node_B;

   private static String DC1_NODEA_URI = "tcp://localhost:61616";
   private static String DC1_NODEB_URI = "tcp://localhost:61617";
   private static String DC2_NODEA_URI = "tcp://localhost:61618";
   private static String DC2_NODEB_URI = "tcp://localhost:61619";

   private static void createServer(String serverName, String configuration, int portOffset, boolean paging) throws Exception {
      File instanceLocation = getFileServerLocation(serverName);
      deleteDirectory(instanceLocation);

      HelperCreate cliCreateServer = helperCreate();
      cliCreateServer.setPortOffset(portOffset);
      cliCreateServer.setAllowAnonymous(true).setUser("A").setPassword("A").setRole("guest").setNoWeb(true).setConfiguration(configuration).setArtemisInstance(instanceLocation);
      cliCreateServer.createServer();
   }

   public static void createRealServers(boolean paging) throws Exception {
      createServer(DC1_NODE_A, "./src/main/resources/servers/federatedMirror/DC1/A", 0, paging);
      createServer(DC1_NODE_B, "./src/main/resources/servers/federatedMirror/DC1/B", 1, paging);
      createServer(DC2_NODE_A, "./src/main/resources/servers/federatedMirror/DC2/A", 2, paging);
      createServer(DC2_NODE_B, "./src/main/resources/servers/federatedMirror/DC2/B", 3, paging);
   }

   private void startServers() throws Exception {
      processDC1_node_A = startServer(DC1_NODE_A, -1, -1, null);
      processDC1_node_B = startServer(DC1_NODE_B, -1, -1, null);
      processDC2_node_A = startServer(DC2_NODE_A, -1, -1, null);
      processDC2_node_B = startServer(DC2_NODE_B, -1, -1, null);

      ServerUtil.waitForServerToStart(0, 10_000);
      ServerUtil.waitForServerToStart(1, 10_000);
      ServerUtil.waitForServerToStart(2, 10_000);
      ServerUtil.waitForServerToStart(3, 10_000);
   }

   @Test
   public void testSimpleQueue() throws Exception {
      createRealServers(false);
      startServers();

      final int numberOfMessages = 200;

      assertTrue(numberOfMessages % 2 == 0, "numberOfMessages must be even");

      ConnectionFactory connectionFactoryDC1A = CFUtil.createConnectionFactory("amqp", DC1_NODEA_URI);
      ConnectionFactory connectionFactoryDC1B = CFUtil.createConnectionFactory("amqp", DC1_NODEB_URI);
      ConnectionFactory connectionFactoryDC2A = CFUtil.createConnectionFactory("amqp", DC2_NODEA_URI);
      String snfQueue = "$ACTIVEMQ_ARTEMIS_MIRROR_mirror";

      SimpleManagement simpleManagementDC1A = new SimpleManagement(DC1_NODEA_URI, null, null);

      try (Connection connection = connectionFactoryDC1A.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("myQueue");
         MessageProducer producer = session.createProducer(queue);

         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage message;
            boolean large;
            if (i % 1 == 2) {
               message = session.createTextMessage(largeBody);
               large = true;
            } else {
               message = session.createTextMessage(smallBody);
               large = false;
            }
            message.setIntProperty("i", i);
            message.setBooleanProperty("large", large);
            producer.send(message);
            if (i % 100 == 0) {
               logger.debug("commit {}", i);
               session.commit();
            }
         }
         session.commit();
      }

      logger.debug("All messages were sent");

      try (Connection connection = connectionFactoryDC1B.createConnection()) {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("myQueue");
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < numberOfMessages / 2; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertNotNull(message);
            logger.info("Received message {}, large={}", message.getIntProperty("i"), message.getBooleanProperty("large"));
         }
         session.commit();
      }
   }
}
