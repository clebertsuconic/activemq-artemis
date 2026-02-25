/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class OOMEReproducer extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected ActiveMQServer server;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(true, createDefaultNettyConfig());

      server.getConfiguration().addAddressSetting("#", new AddressSettings()
         .setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE)
         .setMaxSizeBytes(1024 * 1024)
      ).setJournalSyncTransactional(false).setJournalDatasync(false).setJournalSyncNonTransactional(false);

      server.start();
   }

   @Test
   public void reproducerForMgmntOperationOOMETest() throws Exception {
      //Increase this value until the test causes an OOME
      //The amount will depend on the heap size
      int messageCount = 200000;
      String queueName = "simpleTest";

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      Connection connection = factory.createConnection();
      connection.start();

      Session session = connection.createSession(Session.SESSION_TRANSACTED);
      MessageProducer producer = session.createProducer(session.createQueue(queueName));
      TextMessage message = session.createTextMessage(RandomUtil.randomAlphaNumericString(1024 * 40));

      for (int i = 0; i < messageCount; i++) {
         producer.send(message);

         if (i % 1000 == 0) {
            logger.info("sent {} out of {}", i, messageCount);
            session.commit();
         }

      }

      session.commit();
      producer.close();

      Queue queue = server.locateQueue(queueName);
      assertNotNull(queue);

      Wait.assertEquals(messageCount, queue::getMessageCount, 5000);

      //This is what triggers the OOME
      assertDoesNotThrow(() -> {
         queue.changeReferencesPriority(null, (byte) 2);
      });
      assertDoesNotThrow(() -> {
         queue.deleteMatchingReferences(null);
      });

      //Rest validates that the operation succeeded if no OOME
      MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

      Message msg;
      for (int i = 0; i < messageCount; i++) {
         msg = consumer.receive(1000);
         assertNotNull(msg);
         assertEquals(2, msg.getJMSPriority());
         if (i % 100 == 0) {
            session.commit();
         }
      }

      assertNull(consumer.receiveNoWait());

      session.commit();
      consumer.close();
      session.close();
      connection.close();
   }

}
