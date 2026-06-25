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
package org.apache.activemq.artemis.tests.integration.quota;

import org.apache.activemq.artemis.api.core.ActiveMQResourceQuotaExceededException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuotaConfig;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests demonstrating current limitations where byte quota never decrements
 * when messages are consumed/acknowledged.
 *
 * EXPECTED BEHAVIOR:
 * - Quota increments when messages are routed
 * - Quota decrements when messages are acknowledged/consumed
 * - Quota represents ACTIVE resource usage (unconsumed messages)
 *
 * CURRENT BEHAVIOR (BUG):
 * - Quota increments when messages are routed
 * - Quota NEVER decrements
 * - Quota acts as a "high water mark" usage counter
 * - Eventually quota fills and blocks sends even after all messages consumed
 */
public class QuotaDecrementLimitationsTest extends ActiveMQTestBase {

   /**
    * Test demonstrating that quota doesn't decrement after message consumption.
    *
    * EXPECTED: After consuming all messages, quota returns to 0 and we can send more
    * ACTUAL: Quota stays at high water mark, eventually fills and blocks sends
    */
   @Test
   public void testQuotaDoesNotDecrementOnConsumption() throws Exception {
      Configuration config = createDefaultConfig(false);

      // Small quota to demonstrate the problem quickly
      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxMessageBytes(3000L); // 3KB
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      settings.setMaxSizeBytes(-1L);
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         // Use CLIENT_ACKNOWLEDGE mode (false, false, false) to ensure server-side acknowledge() is called
         ClientSession session = sf.createSession(false, false, false);

         SimpleString address = SimpleString.of("test.consume");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.consume")
            .setAddress(address)
            .setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);
         ClientConsumer consumer = session.createConsumer("test.consume");

         ResourceQuota quota = server.getResourceQuotaService().getQuotaByName("test-quota");
         assertNotNull(quota);

         long initialQuota = quota.getCurrentMessageBytes();
         assertEquals(0, initialQuota, "Quota should start at 0");

         // Send 3 messages (~700 bytes each with overhead = ~2100 bytes total)
         for (int i = 0; i < 3; i++) {
            ClientMessage message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[300]);
            producer.send(message);
         }
         session.commit();  // Commit sends (session is non-transacted but needs explicit commit)

         long afterSend = quota.getCurrentMessageBytes();
         assertTrue(afterSend > 1500, "Quota should increase after sending. Got: " + afterSend);

         // Consume all messages
         session.start();
         for (int i = 0; i < 3; i++) {
            ClientMessage received = consumer.receive(5000);
            assertNotNull(received, "Should receive message " + i);
            received.acknowledge();
         }
         session.commit();  // Commit acknowledgments

         long afterConsume = quota.getCurrentMessageBytes();

         // FIXED: Quota should decrement after consumption
         // After consuming all messages, quota should be close to 0
         assertTrue(afterConsume < 200,
            "Quota should decrement to near 0 after consuming all messages. Got: " + afterConsume);

         // Because quota decrements, we should be able to send more messages
         int additionalMessagesSent = 0;
         try {
            for (int i = 0; i < 10; i++) {
               ClientMessage message = session.createMessage(true);
               message.getBodyBuffer().writeBytes(new byte[300]);
               producer.send(message);
               additionalMessagesSent++;
            }
         } catch (ActiveMQResourceQuotaExceededException e) {
            // Should not happen - quota was freed by consumption
         }

         // FIXED: Should be able to send many more messages since quota was freed
         assertTrue(additionalMessagesSent >= 3,
            "Should be able to send many messages because quota was freed. Sent: " + additionalMessagesSent);

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }

   /**
    * Test demonstrating quota "high water mark" behavior.
    * Quota tracks maximum concurrent usage, not current usage.
    */
   @Test
   public void testQuotaActsAsHighWaterMark() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("hwm-quota");
      quotaConfig.setMaxMessageBytes(10240L); // 10KB
      config.addResourceQuotaConfig("hwm-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("hwm-quota");
      settings.setMaxSizeBytes(-1L);
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         // Use CLIENT_ACKNOWLEDGE mode to ensure server-side acknowledge() is called
         ClientSession session = sf.createSession(false, false, false);

         SimpleString address = SimpleString.of("test.hwm");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.hwm")
            .setAddress(address)
            .setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);
         ClientConsumer consumer = session.createConsumer("test.hwm");
         session.start();

         ResourceQuota quota = server.getResourceQuotaService().getQuotaByName("hwm-quota");

         // Pattern: send 5, consume 5, send 5, consume 5...
         // Expected: quota stays low (~5 messages worth)
         // Now that quota decrements, this should work correctly

         for (int round = 0; round < 5; round++) {
            // Send 5 messages
            for (int i = 0; i < 5; i++) {
               ClientMessage message = session.createMessage(true);
               message.getBodyBuffer().writeBytes(new byte[500]);
               producer.send(message);
            }
            session.commit();

            long afterSend = quota.getCurrentMessageBytes();

            // Consume all 5 messages
            for (int i = 0; i < 5; i++) {
               ClientMessage received = consumer.receive(5000);
               assertNotNull(received);
               received.acknowledge();
            }
            session.commit();

            long afterConsume = quota.getCurrentMessageBytes();

            // FIXED: Quota should decrease after consumption
            assertTrue(afterConsume < afterSend,
               "Round " + round + ": Quota should decrease after consumption. Before: " +
               afterSend + ", After: " + afterConsume);
         }

         // After 5 rounds of send/consume, quota should still be low (just current messages)
         long finalQuota = quota.getCurrentMessageBytes();
         long expectedMaxQuota = 5 * 1000; // ~5 messages worth with overhead

         assertTrue(finalQuota < expectedMaxQuota,
            "FIXED: Quota should stay low (~" + expectedMaxQuota +
            " for 5 messages) and is " + finalQuota);

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }

   /**
    * Test demonstrating quota exhaustion even with continuous consumption.
    *
    * Scenario: Producer sends 1 message/sec, consumer consumes 1 message/sec
    * Expected: Quota stays stable (1 message worth)
    * Actual: Quota grows until exhausted
    */
   @Test
   public void testQuotaExhaustsWithContinuousConsumption() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("exhaust-quota");
      quotaConfig.setMaxMessageBytes(5120L); // 5KB
      config.addResourceQuotaConfig("exhaust-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("exhaust-quota");
      settings.setMaxSizeBytes(-1L);
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         // Use CLIENT_ACKNOWLEDGE mode to ensure server-side acknowledge() is called
         ClientSession session = sf.createSession(false, false, false);

         SimpleString address = SimpleString.of("test.exhaust");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.exhaust")
            .setAddress(address)
            .setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);
         ClientConsumer consumer = session.createConsumer("test.exhaust");
         session.start();

         ResourceQuota quota = server.getResourceQuotaService().getQuotaByName("exhaust-quota");

         // Interleave send/consume - simulating balanced producer/consumer
         // Expected: quota stays low and allows unlimited throughput
         int messagesProcessed = 0;

         for (int i = 0; i < 100; i++) {
            // Send one message
            ClientMessage message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[300]);
            producer.send(message);
            session.commit();

            // Immediately consume it
            ClientMessage received = consumer.receive(5000);
            assertNotNull(received);
            received.acknowledge();
            session.commit();

            messagesProcessed++;
         }

         // FIXED: With balanced send/consume, quota should allow unlimited throughput
         assertEquals(100, messagesProcessed,
            "FIXED: Quota should allow all 100 messages with balanced send/consume");

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }

   /**
    * Test demonstrating the impact on throughput.
    * Shows that quota becomes a throughput limit, not just a concurrent usage limit.
    */
   @Test
   public void testQuotaLimitsThroughputNotJustConcurrency() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("throughput-quota");
      quotaConfig.setMaxMessageBytes(3000L); // 3KB
      config.addResourceQuotaConfig("throughput-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("throughput-quota");
      settings.setMaxSizeBytes(-1L);
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         // Use CLIENT_ACKNOWLEDGE mode to ensure server-side acknowledge() is called
         ClientSession session = sf.createSession(false, false, false);

         SimpleString address = SimpleString.of("test.throughput");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.throughput")
            .setAddress(address)
            .setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);
         ClientConsumer consumer = session.createConsumer("test.throughput");
         session.start();

         ResourceQuota quota = server.getResourceQuotaService().getQuotaByName("throughput-quota");

         // Measure: How many messages can we send/consume before quota blocks?
         // With proper decrement: unlimited (quota = max concurrent, not total)

         int totalProcessed = 0;

         for (int i = 0; i < 1000; i++) {
            ClientMessage message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[300]);
            producer.send(message);
            session.commit();

            ClientMessage received = consumer.receive(5000);
            assertNotNull(received);
            received.acknowledge();
            session.commit();

            totalProcessed++;
         }

         // FIXED: Quota should allow unlimited throughput (just limits concurrent usage)
         assertEquals(1000, totalProcessed,
            "FIXED: Quota should allow unlimited send/consume cycles. Processed: " + totalProcessed);

         // Quota should be low (just whatever's currently in flight, which is ~0)
         long quotaUsage = quota.getCurrentMessageBytes();
         assertTrue(quotaUsage < 500,
            "Quota should be near 0 (" + quotaUsage + "/" + quota.getMaxMessageBytes() + ") after consuming everything");

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }
}
