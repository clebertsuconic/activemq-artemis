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

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.ExecuteUtil;
import org.apache.activemq.artemis.utils.FileUtil;
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
      File artemisScript = new File(serverLocation, "bin/artemis");

      // Start consumer first with durable subscription
      String[] consumerCmd = new String[]{
         artemisScript.getAbsolutePath(),
         "consumer",
         "--destination", "topic://testTopic",
         "--durable",
         "--clientID", "tomr",
         "--commit-interval", String.valueOf(100),
         "--message-count", String.valueOf(MESSAGE_COUNT),
         "--threads", String.valueOf(THREADS),
         "--protocol", "amqp"
      };

      logger.info("Starting consumer...");
      ExecuteUtil.ProcessHolder consumerProcess = ExecuteUtil.run(true, consumerCmd);


      // Give consumer time to connect and subscribe
      Thread.sleep(1000);

      // Start producer
      String[] producerCmd = new String[]{
         artemisScript.getAbsolutePath(),
         "producer",
         "--destination", "topic://testTopic",
         "--message-count", String.valueOf(MESSAGE_COUNT),
         "--commit-interval", String.valueOf(10),
         "--message-size", String.valueOf(MESSAGE_SIZE),
         "--threads", String.valueOf(THREADS),
         "--protocol", "amqp"
      };

      logger.info("Starting producer...");
      ExecuteUtil.ProcessHolder producerProcess = ExecuteUtil.run(true, producerCmd);

      // Wait for both processes to complete
      logger.info("Waiting for producer to complete...");
      int producerExit = producerProcess.waitFor(5, TimeUnit.MINUTES);
      logger.info("Producer exited with code: {}", producerExit);

      logger.info("Waiting for consumer to complete...");
      int consumerExit = consumerProcess.waitFor(5, TimeUnit.MINUTES);
      logger.info("Consumer exited with code: {}", consumerExit);

      // Check server logs for IllegalArgumentException
      File logFile = new File(serverLocation, "log/artemis.log");
      boolean hasIllegalArgumentException = findLogRecord(logFile, "IllegalArgumentException");

      // Assert that no IllegalArgumentException was thrown
      assertFalse(hasIllegalArgumentException,
                  "Server log contains IllegalArgumentException - the bug is still present");
   }
}
