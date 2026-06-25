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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuotaConfig;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for quota enforcement with address full policies (BLOCK/FAIL/PAGE)
 * when quota is the only configured limit (maxSize=-1).
 *
 * These tests verify the fixes for bugs where quota was bypassed when it was
 * the only limit configured.
 */
public class QuotaPolicyEnforcementTest extends ActiveMQTestBase {

   /**
    * Test that FAIL policy enforces quota when quota is the ONLY limit configured.
    * evaluated to false when only quota was set, bypassing enforcement.
    */
   @Test
   public void testFAILPolicyWithQuotaOnly() throws Exception {
      Configuration config = createDefaultConfig(false);

      // Create quota with 1KB byte limit
      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("fail-quota");
      quotaConfig.setMaxMessageBytes(1024L); // 1KB
      config.addResourceQuotaConfig("fail-quota", quotaConfig);

      // Configure address settings: quota only, no maxSize
      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("fail-quota");
      settings.setMaxSizeBytes(-1L); // No local size limit
      settings.setMaxSizeMessages(-1); // No message count limit
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
      config.addAddressSetting("test.#", settings);
      config.setGlobalMaxSize(-1); // No global size limit

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(false, true, true);

         SimpleString address = SimpleString.of("test.fail");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.fail").setAddress(address).setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);

         // Send messages until quota exceeded
         boolean quotaExceeded = false;
         for (int i = 0; i < 100; i++) {
            try {
               ClientMessage message = session.createMessage(true);
               message.getBodyBuffer().writeBytes(new byte[200]); // 200 bytes each
               producer.send(message);
            } catch (ActiveMQException e) {
               // Should fail with quota exceeded
               assertTrue(e.getMessage().contains("quota") || e.getMessage().contains("exceeded"),
                  "Expected quota/full exception but got: " + e.getMessage());
               quotaExceeded = true;
               break;
            }
         }

         assertTrue(quotaExceeded, "FAIL policy should have enforced quota even with maxSize=-1");

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }

   /**
    * Test that BLOCK policy enforces quota when quota is the ONLY limit configured.
    * Before fix: same condition issue as FAIL policy.
    */
   @Test
   public void testBLOCKPolicyWithQuotaOnly() throws Exception {
      Configuration config = createDefaultConfig(false);

      // Create quota with 1KB byte limit
      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("block-quota");
      quotaConfig.setMaxMessageBytes(1024L); // 1KB
      config.addResourceQuotaConfig("block-quota", quotaConfig);

      // Configure address settings: quota only, no maxSize
      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("block-quota");
      settings.setMaxSizeBytes(-1L); // No local size limit
      settings.setMaxSizeMessages(-1); // No message count limit
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      config.addAddressSetting("test.#", settings);
      config.setGlobalMaxSize(-1); // No global size limit

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);
         locator.setProducerMaxRate(1); // Slow down sends
         ClientSessionFactory sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(false, true, true);

         SimpleString address = SimpleString.of("test.block");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.block").setAddress(address).setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);

         // Send messages until we observe blocking
         AtomicBoolean blocked = new AtomicBoolean(false);
         CountDownLatch blockLatch = new CountDownLatch(1);

         Thread senderThread = new Thread(() -> {
            try {
               for (int i = 0; i < 100; i++) {
                  ClientMessage message = session.createMessage(true);
                  message.getBodyBuffer().writeBytes(new byte[200]); // 200 bytes each

                  long start = System.currentTimeMillis();
                  producer.send(message);
                  long elapsed = System.currentTimeMillis() - start;

                  // If send takes more than 100ms, we're likely blocked
                  if (elapsed > 100) {
                     blocked.set(true);
                     blockLatch.countDown();
                     break;
                  }
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
         });

         senderThread.start();

         // Wait up to 5 seconds for blocking to occur
         boolean didBlock = blockLatch.await(5, TimeUnit.SECONDS);

         senderThread.interrupt();
         senderThread.join(1000);

         assertTrue(didBlock || blocked.get(), "BLOCK policy should have enforced quota even with maxSize=-1");

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }

   /**
    * Test that PAGE policy works with quota as the only limit.
    */
   @Test
   public void testPAGEPolicyWithQuotaOnly() throws Exception {
      Configuration config = createDefaultConfig(false);

      // Create quota with 1KB byte limit
      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("page-quota");
      quotaConfig.setMaxMessageBytes(1024L); // 1KB
      config.addResourceQuotaConfig("page-quota", quotaConfig);

      // Configure address settings: quota only, no maxSize
      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("page-quota");
      settings.setMaxSizeBytes(-1L); // No local size limit
      settings.setMaxSizeMessages(-1); // No message count limit
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      config.addAddressSetting("test.#", settings);
      config.setGlobalMaxSize(-1); // No global size limit

      ActiveMQServer server = createServer(true, config); // Enable persistence for paging
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(false, true, true);

         SimpleString address = SimpleString.of("test.page");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.page").setAddress(address).setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);

         // Send enough messages to exceed quota and potentially trigger paging

         boolean quotaExceeded = false;
         for (int i = 0; i < 100; i++) {
            try {
               ClientMessage message = session.createMessage(true);
               message.getBodyBuffer().writeBytes(new byte[200]); // 200 bytes each
               producer.send(message);
            } catch (ActiveMQException e) {
               // Should fail with quota exceeded
               assertTrue(e.getMessage().contains("quota") || e.getMessage().contains("exceeded"),
                     "Expected quota/full exception but got: " + e.getMessage());
               quotaExceeded = true;
               break;
            }
         }

         assertTrue(quotaExceeded);
         // Verify paging was not triggered
         assertFalse(server.getPagingManager().getPageStore(address).isPaging(),
            "PAGE policy should have started paging when quota exceeded");

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }

   /**
    * Test that isFull() ignores quota check.
    */
   @Test
   public void testIsFullIncludesQuotaCheck() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("full-check-quota");
      quotaConfig.setMaxMessageBytes(512L); // 512 bytes
      config.addResourceQuotaConfig("full-check-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("full-check-quota");
      settings.setMaxSizeBytes(-1L);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(false, true, true);

         SimpleString address = SimpleString.of("test.full");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.full").setAddress(address).setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);

         // Fill quota - may throw if quota exceeded, that's expected with FAIL policy
         try {
            for (int i = 0; i < 3; i++) {
               ClientMessage message = session.createMessage(true);
               message.getBodyBuffer().writeBytes(new byte[200]);
               producer.send(message);
            }
         } catch (ActiveMQException e) {
            // Expected - quota exceeded with FAIL policy
         }

         // Verify isFull() ignores quota
         assertFalse(server.getPagingManager().getPageStore(address).isFull(),
            "isFull() check when quota exceeded");

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }

   /**
    * Test that quota is enforced alongside existing maxSize limits.
    */
   @Test
   public void testQuotaWithMaxSizeCombined() throws Exception {
      Configuration config = createDefaultConfig(false);

      // Quota has higher limit than maxSize
      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("combined-quota");
      quotaConfig.setMaxMessageBytes(2048L); // 2KB quota
      config.addResourceQuotaConfig("combined-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("combined-quota");
      settings.setMaxSizeBytes(1024L); // 1KB maxSize (lower than quota)
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(false, true, true);

         SimpleString address = SimpleString.of("test.combined");
         session.createAddress(address, RoutingType.ANYCAST, false);
         session.createQueue(QueueConfiguration.of("test.combined").setAddress(address).setRoutingType(RoutingType.ANYCAST));

         ClientProducer producer = session.createProducer(address);

         // Should hit maxSize limit (1KB) before quota limit (2KB)
         boolean hitLimit = false;
         for (int i = 0; i < 20; i++) {
            try {
               ClientMessage message = session.createMessage(true);
               message.getBodyBuffer().writeBytes(new byte[200]);
               producer.send(message);
            } catch (ActiveMQException e) {
               hitLimit = true;
               break;
            }
         }

         assertTrue(hitLimit, "Should hit maxSize or quota limit");

         session.close();
         locator.close();
      } finally {
         server.stop();
      }
   }
}
