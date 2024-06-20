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

package org.apache.activemq.artemis.tests.soak.stomp;

import java.io.File;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompSoakTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_0 = "stomp/stompServer";

   private static final int THREADS = 10;
   private static final int NUMBER_OF_MESSAGES = 1000;

   Process serverProcess;

   @BeforeAll
   public static void createServer() throws Exception {
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_0);
         deleteDirectory(serverLocation);

         StringWriter queuesWriter = new StringWriter();

         for (int i = 0; i < THREADS; i++) {
            if (i > 0)
               queuesWriter.write(",");
            queuesWriter.write("CLIENT_" + i);
            ;
         }

         HelperCreate cliCreateServer = new HelperCreate();
         cliCreateServer.setRole("amq").setUser("admin").setPassword("admin").setAllowAnonymous(true).setNoWeb(false).setArtemisInstance(serverLocation);
         cliCreateServer.addArgs("--queues", queuesWriter.toString());
         // some limited memory to make it more likely to fail
         cliCreateServer.setArgs("--java-memory", "512M");
         cliCreateServer.createServer();
      }
   }

   @Test
   public void testStomp() throws Exception {
      serverProcess = startServer(SERVER_NAME_0, 0, 60_000);

      ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
      runAfter(executorService::shutdownNow);

      CountDownLatch done = new CountDownLatch(THREADS);
      AtomicInteger errors = new AtomicInteger(0);

      for (int i = 0; i < THREADS; i++) {
         String destination = "CLIENT_" + i;
         executorService.execute(() -> {
            StompClientConnection clientConnection = null;
            try {
               clientConnection = StompClientConnectionFactory.createClientConnection(new URI("tcp+v12.stomp://localhost:61616"));

               clientConnection.connect();

               ClientStompFrame subscribeFrame = clientConnection.createFrame(Stomp.Commands.SUBSCRIBE).addHeader(Stomp.Headers.Subscribe.SUBSCRIPTION_TYPE, RoutingType.ANYCAST.toString()).addHeader(Stomp.Headers.Subscribe.DESTINATION, destination);

               clientConnection.sendFrame(subscribeFrame);

               for (int messageCount = 0; messageCount < NUMBER_OF_MESSAGES; messageCount++) {

                  String txId = "tx" + messageCount + "_" + destination;

                  ClientStompFrame beginFrame = clientConnection.createFrame(Stomp.Commands.BEGIN).addHeader(Stomp.Headers.TRANSACTION, txId);

                  clientConnection.sendFrame(beginFrame);

                  ClientStompFrame frame = clientConnection.createFrame(Stomp.Commands.SEND).addHeader(Stomp.Headers.Send.DESTINATION, destination).setBody("message" + messageCount).addHeader(Stomp.Headers.TRANSACTION, txId).addHeader(Stomp.Headers.Send.PERSISTENT, Boolean.TRUE.toString());

                  for (int repeat = 0; repeat < 10; repeat++) {
                     clientConnection.sendFrame(frame);
                  }

                  {
                     ClientStompFrame commitFrame = clientConnection.createFrame(Stomp.Commands.COMMIT).addHeader(Stomp.Headers.TRANSACTION, txId);
                     clientConnection.sendFrame(commitFrame);
                  }

                  beginFrame = clientConnection.createFrame(Stomp.Commands.BEGIN).addHeader(Stomp.Headers.TRANSACTION, "receive" + txId);

                  clientConnection.sendFrame(beginFrame);

                  for (int repeat = 0; repeat < 10; repeat++) {
                     ClientStompFrame receivedFrame = clientConnection.receiveFrame();
                     Assertions.assertEquals("MESSAGE", receivedFrame.getCommand());
                     Assertions.assertEquals("message" + messageCount, receivedFrame.getBody());
                  }

                  {
                     ClientStompFrame commitFrame = clientConnection.createFrame(Stomp.Commands.COMMIT).addHeader(Stomp.Headers.TRANSACTION, "receive" + txId);
                     clientConnection.sendFrame(commitFrame);
                  }
               }

            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();

            } finally {
               try {
                  clientConnection.closeTransport();
                  clientConnection.disconnect();
               } catch (Throwable ignored) {
               }

               done.countDown();
            }

         });
      }

      Assertions.assertTrue(done.await(10, TimeUnit.MINUTES));
      Assertions.assertEquals(0, errors.get());
   }

}
