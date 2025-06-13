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
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.File;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LargeAccumulationTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static String body;

   static {
      StringWriter writer = new StringWriter();
      while (writer.getBuffer().length() < 10 * 1024) {
         writer.append("This is a string ..... ");
      }
      body = writer.toString();
   }

   private static final String QUEUE_NAME = "LargeQueue";

   public static final String DC1_NODE_A = "LargeAccumulationTest/DC1";
   public static final String DC2_NODE_A = "LargeAccumulationTest/DC2";

   private static final String SNF_QUEUE = "$ACTIVEMQ_ARTEMIS_MIRROR_mirror";

   Process processDC1_node_A;
   Process processDC2_node_A;

   private static String DC1_NODEA_URI = "tcp://localhost:61616";
   private static String DC2_NODEA_URI = "tcp://localhost:61618";

   private static void createServer(String serverName,
                                    String connectionName,
                                    String mirrorURI,
                                    int portOffset) throws Exception {
      File serverLocation = getFileServerLocation(serverName);
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = helperCreate();
      cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
      cliCreateServer.setMessageLoadBalancing("ON_DEMAND");
      cliCreateServer.setClustered(false);
      cliCreateServer.setNoWeb(false);
      cliCreateServer.setArgs("--no-stomp-acceptor", "--no-hornetq-acceptor", "--no-mqtt-acceptor", "--no-amqp-acceptor", "--max-hops", "1", "--name", DC1_NODE_A);
      cliCreateServer.addArgs("--queues", QUEUE_NAME);
      cliCreateServer.addArgs("--java-memory", "512M");
      cliCreateServer.setPortOffset(portOffset);
      cliCreateServer.createServer();

      Properties brokerProperties = new Properties();
      brokerProperties.put("AMQPConnections." + connectionName + ".uri", mirrorURI);
      brokerProperties.put("AMQPConnections." + connectionName + ".retryInterval", "1000");
      brokerProperties.put("AMQPConnections." + connectionName + ".type", AMQPBrokerConnectionAddressType.MIRROR.toString());
      brokerProperties.put("AMQPConnections." + connectionName + ".connectionElements.mirror.sync", "false");
      brokerProperties.put("largeMessageSync", "false");
      brokerProperties.put("pageSyncTimeout", "20000");
      brokerProperties.put("messageExpiryScanPeriod", "-1");

      brokerProperties.put("addressSettings.#.maxSizeMessages", "1");
      brokerProperties.put("addressSettings.#.addressFullMessagePolicy", "PAGING");
      brokerProperties.put("addressSettings.#.maxReadPageMessages", "100");
      brokerProperties.put("addressSettings.#.maxReadPageBytes", "-1");
      brokerProperties.put("addressSettings.#.prefetchPageMessages", "10");
      brokerProperties.put("mirrorPageTransaction", "true");
      brokerProperties.put("mirrorAckManagerPageAttempts", "40");

      File brokerPropertiesFile = new File(serverLocation, "broker.properties");
      saveProperties(brokerProperties, brokerPropertiesFile);
   }

   @BeforeAll
   public static void createServers() throws Exception {
      createServer(DC1_NODE_A, "mirror", DC2_NODEA_URI, 0);
      createServer(DC2_NODE_A, "mirror", DC1_NODEA_URI, 2);
   }

   @BeforeEach
   public void cleanupServers() {
      cleanupData(DC1_NODE_A);
      cleanupData(DC2_NODE_A);
   }

   public CountDownLatch send(Executor executor,
                              AtomicInteger errors,
                              int threads,
                              ConnectionFactory connectionFactory,
                              int numberOfMessgesPerThread,
                              int sizePerMessage) {
      CountDownLatch done = new CountDownLatch(threads);
      AtomicInteger messageSent = new AtomicInteger(0);
      String body = "a".repeat(sizePerMessage);
      for (int i = 0; i < threads; i++) {
         boolean useTX = i % 2 == 0;
         executor.execute(() -> {
            try (Connection connection = connectionFactory.createConnection()) {
               Session session;

               if (useTX) {
                  session = connection.createSession(true, Session.SESSION_TRANSACTED);
               } else {
                  session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               }
               MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
               for (int m = 0; m < numberOfMessgesPerThread; m++) {
                  producer.send(session.createTextMessage(body));
                  if (useTX) {
                     session.commit();
                  }
                  int sent = messageSent.incrementAndGet();
                  if (sent % 100 == 0) {
                     logger.info("message sent {}", sent);
                  }
               }
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            } finally {
               done.countDown();
            }
         });
      }
      return done;
   }

   @Test
   public void testLargeAccumulation() throws Exception {
      AtomicInteger errors = new AtomicInteger(0);
      int threads = 30;
      ExecutorService service = Executors.newFixedThreadPool(threads);
      runAfter(service::shutdownNow);

      String protocol = "AMQP";
      startDC1();

      final int numberOfLargeMessages = 1_000; // 5_000 it's the original
      final int sizeOfLargeMessage = 200_000;
      final int numberOfMediumMessages = 1_000; // 50_000 it's the original
      final int sizeOfMediumMessage = 30_000;

      ConnectionFactory[] cfs = new ConnectionFactory[]{CFUtil.createConnectionFactory(protocol, DC1_NODEA_URI), CFUtil.createConnectionFactory(protocol, DC2_NODEA_URI)};
      SimpleManagement[] sm = new SimpleManagement[]{new SimpleManagement(DC1_NODEA_URI, null, null), new SimpleManagement(DC2_NODEA_URI, null, null)};

      CountDownLatch done = send(service, errors, threads, cfs[0], numberOfLargeMessages, sizeOfLargeMessage);
      assertTrue(done.await(10, TimeUnit.MINUTES));
      assertEquals(0, errors.get());
      startDC2();
      matchMessageCounts(sm, (long) numberOfLargeMessages * threads);

      done = send(service, errors, threads, cfs[0], numberOfMediumMessages, sizeOfMediumMessage);
      assertTrue(done.await(60, TimeUnit.MINUTES));
      assertEquals(0, errors.get());
      matchMessageCounts(sm, (long) (numberOfLargeMessages + numberOfMediumMessages) * threads);

   }

   private static void matchMessageCounts(SimpleManagement[] sm, long numberOfLargeMessages) throws Exception {
      for (SimpleManagement s : sm) {
         logger.debug("Checking counts on SNF for {}", s.getUri());
         Wait.assertEquals((long) 0, () -> s.getMessageCountOnQueue(SNF_QUEUE), 120_000, 100);
         logger.debug("Checking counts on {} on {}", QUEUE_NAME, s.getUri());
         Wait.assertEquals(numberOfLargeMessages, () -> s.getMessageCountOnQueue(QUEUE_NAME), 60_000, 100);
      }
   }

   int getNumberOfLargeMessages(String serverName) throws Exception {
      File lmFolder = new File(getServerLocation(serverName) + "/data/large-messages");
      assertTrue(lmFolder.exists());
      return lmFolder.list().length;
   }

   private void startDC1() throws Exception {
      processDC1_node_A = startServer(DC1_NODE_A, -1, -1, new File(getServerLocation(DC1_NODE_A), "broker.properties"));
      ServerUtil.waitForServerToStart(0, 10_000);
   }

   private void stopDC1() throws Exception {
      processDC1_node_A.destroyForcibly();
      assertTrue(processDC1_node_A.waitFor(10, TimeUnit.SECONDS));
   }

   private void stopDC2() throws Exception {
      processDC2_node_A.destroyForcibly();
      assertTrue(processDC2_node_A.waitFor(10, TimeUnit.SECONDS));
   }

   private void startDC2() throws Exception {
      processDC2_node_A = startServer(DC2_NODE_A, -1, -1, new File(getServerLocation(DC2_NODE_A), "broker.properties"));
      ServerUtil.waitForServerToStart(2, 10_000);
   }
}