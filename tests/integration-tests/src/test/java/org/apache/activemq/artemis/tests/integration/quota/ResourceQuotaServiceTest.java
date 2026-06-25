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
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.quota.AddressQuotaToken;
import org.apache.activemq.artemis.core.server.quota.QueueQuotaToken;
import org.apache.activemq.artemis.core.server.quota.ResourceQuotaService;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuotaConfig;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ResourceQuotaService token-based quota enforcement.
 * Verifies token commit/rollback semantics and integration with server lifecycle.
 */
public class ResourceQuotaServiceTest extends ActiveMQTestBase {

   @Test
   public void testAddressTokenCommit() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(3);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ResourceQuotaService quotaService = server.getResourceQuotaService();
         assertNotNull(quotaService);
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         // Acquire token and commit
         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr1"), false)) {
            // Token acquired, quota incremented
            assertEquals(1, quota.getAddressCount());
            token.commit();
         }

         // After commit, quota should remain incremented
         assertEquals(1, quota.getAddressCount());

         // Acquire second token and commit
         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr2"), false)) {
            assertEquals(2, quota.getAddressCount());
            token.commit();
         }

         assertEquals(2, quota.getAddressCount());

      } finally {
         server.stop();
      }
   }

   @Test
   public void testAddressTokenRollback() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(3);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ResourceQuotaService quotaService = server.getResourceQuotaService();
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         // Acquire token without commit - should rollback on close
         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr1"), false)) {
            assertEquals(1, quota.getAddressCount());
            // No commit - token will rollback
         }

         // After rollback, quota should be back to 0
         assertEquals(0, quota.getAddressCount());

         // Verify we can still acquire tokens
         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr1"), false)) {
            assertEquals(1, quota.getAddressCount());
            token.commit();
         }

         assertEquals(1, quota.getAddressCount());

      } finally {
         server.stop();
      }
   }

   @Test
   public void testQueueTokenCommitAndRollback() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxQueues(5);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ResourceQuotaService quotaService = server.getResourceQuotaService();
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         // Commit scenario
         try (QueueQuotaToken token = quotaService.acquireQueueToken(
               SimpleString.of("test.addr"), false)) {
            assertEquals(1, quota.getQueueCount());
            token.commit();
         }
         assertEquals(1, quota.getQueueCount());

         // Rollback scenario
         try (QueueQuotaToken token = quotaService.acquireQueueToken(
               SimpleString.of("test.addr"), false)) {
            assertEquals(2, quota.getQueueCount());
            // No commit
         }
         assertEquals(1, quota.getQueueCount());

      } finally {
         server.stop();
      }
   }

   @Test
   public void testTokenRollbackOnException() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(3);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ResourceQuotaService quotaService = server.getResourceQuotaService();
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         // Simulate exception during address creation
         try {
            try (AddressQuotaToken token = quotaService.acquireAddressToken(
                  SimpleString.of("test.addr1"), false)) {
               assertEquals(1, quota.getAddressCount());

               // Simulate failure during address creation
               throw new RuntimeException("Simulated creation failure");
            }
         } catch (RuntimeException e) {
            // Expected
         }

         // Quota should have rolled back
         assertEquals(0, quota.getAddressCount());

      } finally {
         server.stop();
      }
   }

   @Test
   public void testReloadModeSkipsQuotaEnforcement() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(2);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ResourceQuotaService quotaService = server.getResourceQuotaService();
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         // With reload=true, quota enforcement should be skipped
         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr1"), true)) {
            assertEquals(1, quota.getAddressCount());
            token.commit();
         }

         assertEquals(1, quota.getAddressCount());

         // Verify we can exceed quota in reload mode
         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr2"), true)) {
            assertEquals(2, quota.getAddressCount());
            token.commit();
         }

         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr3"), true)) {
            assertEquals(3, quota.getAddressCount());
            token.commit();
         }

         // No quota checks in reload mode
         assertEquals(3, quota.getAddressCount());

      } finally {
         server.stop();
      }
   }

   @Test
   public void testTokenExceedsQuotaLimit() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(2);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ResourceQuotaService quotaService = server.getResourceQuotaService();
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         // Acquire and commit 2 tokens (at limit)
         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr1"), false)) {
            token.commit();
         }

         try (AddressQuotaToken token = quotaService.acquireAddressToken(
               SimpleString.of("test.addr2"), false)) {
            token.commit();
         }

         assertEquals(2, quota.getAddressCount());

         // Third token should fail
         ActiveMQResourceQuotaExceededException exception = assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> quotaService.acquireAddressToken(SimpleString.of("test.addr3"), false)
         );

         assertTrue(exception.getMessage().contains("Address quota exceeded"));
         assertTrue(exception.getMessage().contains("test-quota"));
         assertTrue(exception.getMessage().contains("max addresses is 2"));

      } finally {
         server.stop();
      }
   }

   @Test
   public void testEndToEndAddressCreationWithTokens() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(3);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         ResourceQuotaService quotaService = server.getResourceQuotaService();
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         // Create addresses through normal API (uses tokens internally)
         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr1"), RoutingType.ANYCAST));
         assertEquals(1, quota.getAddressCount());

         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr2"), RoutingType.ANYCAST));
         assertEquals(2, quota.getAddressCount());

         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr3"), RoutingType.ANYCAST));
         assertEquals(3, quota.getAddressCount());

         // Fourth should fail
         assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr4"), RoutingType.ANYCAST))
         );

         // Remove one
         server.removeAddressInfo(SimpleString.of("test.addr1"), null);
         assertEquals(2, quota.getAddressCount());

         // Should be able to create another
         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr4"), RoutingType.ANYCAST));
         assertEquals(3, quota.getAddressCount());

      } finally {
         server.stop();
      }
   }

   @Test
   public void testEndToEndQueueCreationWithTokens() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxQueues(3);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         // Create address first
         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr"), RoutingType.ANYCAST));

         // Create queues through normal API (uses tokens internally)
         server.createQueue(QueueConfiguration.of("queue1").setAddress("test.addr"));

         ResourceQuotaService quotaService = server.getResourceQuotaService();
         ResourceQuota quota = quotaService.getQuotaByName("test-quota");
         assertNotNull(quota);

         assertEquals(1, quota.getQueueCount());

         server.createQueue(QueueConfiguration.of("queue2").setAddress("test.addr"));
         assertEquals(2, quota.getQueueCount());

         server.createQueue(QueueConfiguration.of("queue3").setAddress("test.addr"));
         assertEquals(3, quota.getQueueCount());

         // Fourth should fail
         assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> server.createQueue(QueueConfiguration.of("queue4").setAddress("test.addr"))
         );

         // Destroy one
         server.destroyQueue(SimpleString.of("queue1"));
         assertEquals(2, quota.getQueueCount());

         // Should be able to create another
         server.createQueue(QueueConfiguration.of("queue4").setAddress("test.addr"));
         assertEquals(3, quota.getQueueCount());

      } finally {
         server.stop();
      }
   }
}
