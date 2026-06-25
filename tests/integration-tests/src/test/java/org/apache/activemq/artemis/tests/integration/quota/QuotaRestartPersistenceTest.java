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
 * Tests that quota state is correctly rebuilt after server restart.
 * Verifies that quotas survive restart and continue enforcing limits.
 *
 * <p>Architecture: Configuration quotas are immutable templates. On server start,
 * PagingManagerImpl creates fresh runtime instances from these templates (via copy()).
 * Runtime instances start with zero counts, which are rebuilt during journal replay
 * as addresses/queues are reloaded (reload=true triggers count increments).
 *
 * <p>These tests verify runtime instances (from ResourceQuotaManager), NOT config templates.
 */
public class QuotaRestartPersistenceTest extends ActiveMQTestBase {

   @Test
   public void testAddressQuotaRebuildAfterRestart() throws Exception {
      Configuration config = createDefaultConfig(true);  // persistence enabled

      // Create quota with max 3 addresses
      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(3);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      // Configure address settings to use this quota
      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(config);
      server.start();

      try {
         // Create 2 addresses before restart
         AddressInfo addr1 = new AddressInfo(SimpleString.of("test.addr1"), RoutingType.ANYCAST);
         server.addAddressInfo(addr1);

         AddressInfo addr2 = new AddressInfo(SimpleString.of("test.addr2"), RoutingType.ANYCAST);
         server.addAddressInfo(addr2);

         // Verify addresses exist
         assertNotNull(server.getAddressInfo(SimpleString.of("test.addr1")));
         assertNotNull(server.getAddressInfo(SimpleString.of("test.addr2")));

         // Get the RUNTIME quota instance by name (both addresses share same quota)
         ResourceQuota runtimeQuota = server.getResourceQuotaService()
            .getQuotaByName("test-quota");

         assertNotNull(runtimeQuota, "Runtime quota should exist");

         // Verify runtime quota count is 2 before restart
         assertEquals(2, runtimeQuota.getAddressCount(), "Before restart, runtime quota count should be 2");
      } finally {
         server.stop();
      }

      // Restart the server with same configuration
      ActiveMQServer server2 = createServer(config);
      server2.start();

      try {
         // Verify addresses were restored from journal
         assertNotNull(server2.getAddressInfo(SimpleString.of("test.addr1")),
                      "addr1 should be restored after restart");
         assertNotNull(server2.getAddressInfo(SimpleString.of("test.addr2")),
                      "addr2 should be restored after restart");

         // Get the RUNTIME quota instance by name (both addresses share same quota)
         ResourceQuota runtimeQuota = server2.getResourceQuotaService()
            .getQuotaByName("test-quota");

         assertNotNull(runtimeQuota, "Runtime quota should exist");

         // CRITICAL TEST: After restart, quota counts should be rebuilt by reloading addresses
         // The count should be 2 (matching the restored addresses)
         assertEquals(2, runtimeQuota.getAddressCount(),
                     "After restart, quota count should be rebuilt to 2 - THIS IS THE BUG TEST");

         // Should be able to create one more address (limit is 3)
         AddressInfo addr3 = new AddressInfo(SimpleString.of("test.addr3"), RoutingType.ANYCAST);
         server2.addAddressInfo(addr3);
         assertEquals(3, runtimeQuota.getAddressCount(), "After adding addr3, count should be 3");

         // Fourth address should fail (quota limit reached)
         AddressInfo addr4 = new AddressInfo(SimpleString.of("test.addr4"), RoutingType.ANYCAST);
         ActiveMQResourceQuotaExceededException exception = assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> server2.addAddressInfo(addr4),
            "Fourth address should exceed quota limit"
         );
         assertTrue(exception.getMessage().contains("Address quota exceeded"));
         assertTrue(exception.getMessage().contains("test-quota"));

         // Verify final count is still 3 (addr4 was not created)
         assertEquals(3, runtimeQuota.getAddressCount());

      } finally {
         server2.stop();
      }
   }

   @Test
   public void testQueueQuotaRebuildAfterRestart() throws Exception {
      Configuration config = createDefaultConfig(true);  // persistence enabled

      // Create quota with max 4 queues
      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxQueues(4);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(config);
      server.start();

      try {
         // Create address
         AddressInfo addr = new AddressInfo(SimpleString.of("test.addr"), RoutingType.ANYCAST);
         server.addAddressInfo(addr);

         // Create 2 queues before restart
         server.createQueue(QueueConfiguration.of("queue1").setAddress("test.addr").setDurable(true));
         server.createQueue(QueueConfiguration.of("queue2").setAddress("test.addr").setDurable(true));

         // Get the RUNTIME quota instance (not the config template!)
         ResourceQuota runtimeQuota = server.getResourceQuotaService()
            .lookupQuota(SimpleString.of("test.addr"));

         assertNotNull(runtimeQuota, "Runtime quota should exist");

         // Verify runtime quota count before restart
         assertEquals(2, runtimeQuota.getQueueCount(), "Before restart, runtime queue count should be 2");

      } finally {
         server.stop();
      }

      // Restart the server
      ActiveMQServer server2 = createServer(config);
      server2.start();

      try {
         // Verify queues were restored
         assertNotNull(server2.locateQueue(SimpleString.of("queue1")), "queue1 should be restored");
         assertNotNull(server2.locateQueue(SimpleString.of("queue2")), "queue2 should be restored");

         // Get the RUNTIME quota instance by name
         ResourceQuota runtimeQuota = server2.getResourceQuotaService()
            .getQuotaByName("test-quota");

         assertNotNull(runtimeQuota, "Runtime quota should exist");

         // CRITICAL TEST: Quota count should be rebuilt to 2
         assertEquals(2, runtimeQuota.getQueueCount(),
                     "After restart, queue count should be rebuilt to 2");

         // Should be able to create 2 more queues (limit is 4)
         server2.createQueue(QueueConfiguration.of("queue3").setAddress("test.addr").setDurable(true));
         assertEquals(3, runtimeQuota.getQueueCount());

         server2.createQueue(QueueConfiguration.of("queue4").setAddress("test.addr").setDurable(true));
         assertEquals(4, runtimeQuota.getQueueCount());

         // Fifth queue should fail
         ActiveMQResourceQuotaExceededException exception = assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> server2.createQueue(QueueConfiguration.of("queue5").setAddress("test.addr").setDurable(true)),
            "Fifth queue should exceed quota limit"
         );
         assertTrue(exception.getMessage().contains("Queue quota exceeded"));

      } finally {
         server2.stop();
      }
   }

   @Test
   public void testWildcardQuotaRebuildAfterRestart() throws Exception {
      Configuration config = createDefaultConfig(true);  // persistence enabled

      // Create wildcard template quota "region.*" with max 2 addresses per region
      ResourceQuotaConfig regionTemplate = new ResourceQuotaConfig("region.*");
      regionTemplate.setMaxAddresses(2);
      config.addResourceQuotaConfig("region.*", regionTemplate);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("region.*");
      config.addAddressSetting("region.#", settings);

      ActiveMQServer server = createServer(config);
      server.start();

      try {
         // Create 2 addresses in region.us before restart
         server.addAddressInfo(new AddressInfo(SimpleString.of("region.us.orders"), RoutingType.ANYCAST));
         server.addAddressInfo(new AddressInfo(SimpleString.of("region.us.payments"), RoutingType.ANYCAST));

         // Create 1 address in region.eu before restart
         server.addAddressInfo(new AddressInfo(SimpleString.of("region.eu.orders"), RoutingType.ANYCAST));

      } finally {
         server.stop();
      }

      // Restart the server
      ActiveMQServer server2 = createServer(config);
      server2.start();

      try {
         // Verify addresses were restored
         assertNotNull(server2.getAddressInfo(SimpleString.of("region.us.orders")));
         assertNotNull(server2.getAddressInfo(SimpleString.of("region.us.payments")));
         assertNotNull(server2.getAddressInfo(SimpleString.of("region.eu.orders")));

         // Get the RUNTIME quota instances for each region (wildcard creates separate instances)
         ResourceQuota usQuota = server2.getResourceQuotaService()
            .lookupQuota(SimpleString.of("region.us.orders"));
         ResourceQuota euQuota = server2.getResourceQuotaService()
            .lookupQuota(SimpleString.of("region.eu.orders"));

         assertNotNull(usQuota, "US quota instance should exist");
         assertNotNull(euQuota, "EU quota instance should exist");

         // CRITICAL TEST: Each region's quota should be rebuilt
         assertEquals(2, usQuota.getAddressCount(),
                     "region.us count should be 2 after restart");
         assertEquals(1, euQuota.getAddressCount(),
                     "region.eu count should be 1 after restart");

         // region.us should be at limit (2 addresses)
         assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> server2.addAddressInfo(new AddressInfo(SimpleString.of("region.us.shipping"), RoutingType.ANYCAST)),
            "region.us should be at limit after restart"
         );

         // region.eu should have capacity for 1 more
         server2.addAddressInfo(new AddressInfo(SimpleString.of("region.eu.payments"), RoutingType.ANYCAST));
         assertEquals(2, euQuota.getAddressCount());

         // Now region.eu should also be at limit
         assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> server2.addAddressInfo(new AddressInfo(SimpleString.of("region.eu.shipping"), RoutingType.ANYCAST)),
            "region.eu should be at limit after adding second address"
         );

      } finally {
         server2.stop();
      }
   }

   @Test
   public void testHierarchicalQuotaRebuildAfterRestart() throws Exception {
      Configuration config = createDefaultConfig(true);  // persistence enabled

      // Create parent quota with total limit of 5 addresses
      ResourceQuotaConfig parentQuota = new ResourceQuotaConfig("parent");
      parentQuota.setMaxAddresses(5);
      config.addResourceQuotaConfig("parent", parentQuota);

      // Create child quota with higher limit but part of parent
      ResourceQuotaConfig childQuota = new ResourceQuotaConfig("child");
      childQuota.setMaxAddresses(10);
      childQuota.setPartOf("parent");
      config.addResourceQuotaConfig("child", childQuota);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("child");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(config);
      server.start();

      try {
         // Create 3 addresses before restart
         for (int i = 1; i <= 3; i++) {
            server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr" + i), RoutingType.ANYCAST));
         }

         // Get the RUNTIME quota instances (not the config templates!)
         ResourceQuota runtimeChild = server.getResourceQuotaService()
            .getQuotaByName("child");

         assertNotNull(runtimeChild, "Runtime child quota should exist");
         ResourceQuota runtimeParent = runtimeChild.getParent();
         assertNotNull(runtimeParent, "Runtime parent quota should be linked");

         // Verify runtime counts before restart
         assertEquals(3, runtimeChild.getAddressCount(), "Runtime child count before restart");
         assertEquals(3, runtimeParent.getAddressCount(), "Runtime parent count before restart");
      } finally {
         server.stop();
      }

      // Restart the server
      ActiveMQServer server2 = createServer(config);
      server2.start();

      try {
         // Verify addresses were restored
         for (int i = 1; i <= 3; i++) {
            assertNotNull(server2.getAddressInfo(SimpleString.of("test.addr" + i)),
                         "addr" + i + " should be restored");
         }

         // Get the RUNTIME quota instances (not the config templates!)
         ResourceQuota runtimeChild = server2.getResourceQuotaService()
            .getQuotaByName("child");

         assertNotNull(runtimeChild, "Runtime child quota should exist");

         // Get parent from child (they should be linked at runtime)
         ResourceQuota runtimeParent = runtimeChild.getParent();
         assertNotNull(runtimeParent, "Parent quota should be linked");

         // CRITICAL TEST: Quota counts should be rebuilt
         assertEquals(3, runtimeChild.getAddressCount(),
                     "Child count after restart should be 3 - THIS IS THE BUG TEST");
         assertEquals(3, runtimeParent.getAddressCount(),
                     "Parent count after restart should be 3 - THIS IS THE BUG TEST");

         // Should be able to create 2 more (parent limit is 5)
         server2.addAddressInfo(new AddressInfo(SimpleString.of("test.addr4"), RoutingType.ANYCAST));
         server2.addAddressInfo(new AddressInfo(SimpleString.of("test.addr5"), RoutingType.ANYCAST));

         assertEquals(5, runtimeChild.getAddressCount());
         assertEquals(5, runtimeParent.getAddressCount());

         // Sixth address should fail due to parent limit
         assertThrows(
            ActiveMQResourceQuotaExceededException.class,
            () -> server2.addAddressInfo(new AddressInfo(SimpleString.of("test.addr6"), RoutingType.ANYCAST)),
            "Should fail due to parent quota limit"
         );

      } finally {
         server2.stop();
      }
   }
}
