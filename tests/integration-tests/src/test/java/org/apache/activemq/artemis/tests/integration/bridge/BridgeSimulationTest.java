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
package org.apache.activemq.artemis.tests.integration.bridge;

import java.nio.ByteBuffer;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * this test will simulate what a bridge sender would do with auto completes
 */
public class BridgeSimulationTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private final SimpleString address = SimpleString.of("address");

   private final SimpleString queueName = SimpleString.of("queue");

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(true);
      server.start();
   }

   @Test
   public void testSendAcknowledgements() throws Exception {
      ServerLocator locator = createInVMNonHALocator();

      locator.setConfirmationWindowSize(100 * 1024);

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession(null, null, false, true, true, false, 1);

      Queue queue = server.createQueue(QueueConfiguration.of(queueName).setAddress(address).setRoutingType(RoutingType.ANYCAST).setDurable(true));

      ClientProducer prod = session.createProducer(address);

      SendAcknowledgementHandler handler = new SendAcknowledgementHandler() {
         @Override
         public void sendAcknowledged(Message message) {
            System.out.println("Acking " + message);
         }
      };

      session.setSendAcknowledgementHandler(handler);

      ClientMessage message = session.createMessage(true);
      message.putExtraBytesProperty(Message.HDR_ROUTE_TO_IDS, ByteBuffer.allocate(8).putLong(queue.getID()).array());
      prod.send(message);

      Wait.assertEquals(1L, queue::getMessageCount, 5000, 100);

      Thread.sleep(10000);
   }
}
