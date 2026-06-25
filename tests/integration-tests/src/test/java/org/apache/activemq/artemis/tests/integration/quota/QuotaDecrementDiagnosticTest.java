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

/**
 * Diagnostic test to understand quota decrement behavior.
 */
public class QuotaDecrementDiagnosticTest extends ActiveMQTestBase {

   @Test
   public void testSimpleCreateRemoveCreate() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(5);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         System.out.println("=== Creating addr1 ===");
         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr1"), RoutingType.ANYCAST));


         ResourceQuota quota = server.getResourceQuotaService().lookupQuota(SimpleString.of("test.addr1"));
         assertNotNull(quota);

         System.out.println("After create addr1: count = " + quota.getAddressCount());
         assertEquals(1, quota.getAddressCount());

         System.out.println("\n=== Creating addr2 ===");

         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr2"), RoutingType.ANYCAST));

         System.out.println("After create addr2: count = " + quota.getAddressCount());
         quota = server.getResourceQuotaService().lookupQuota(SimpleString.of("test.addr2"));
         assertNotNull(quota);
         assertEquals(2, quota.getAddressCount());

         System.out.println("\n=== Removing addr1 ===");
         server.removeAddressInfo(SimpleString.of("test.addr1"), null);

         quota = server.getResourceQuotaService().lookupQuota(SimpleString.of("test.addr1"));
         assertNotNull(quota);
         System.out.println("After remove addr1: count = " + quota.getAddressCount());
         assertEquals(1, quota.getAddressCount());

         System.out.println("\n=== Creating addr3 ===");
         AddressInfo addr3Info = server.getAddressInfo(SimpleString.of("test.addr3"));
         System.out.println("Before create addr3, existing info: " + addr3Info);

         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr3"), RoutingType.ANYCAST));

         addr3Info = server.getAddressInfo(SimpleString.of("test.addr3"));
         System.out.println("After create addr3, info exists: " + (addr3Info != null));
         System.out.println("After create addr3: count = " + quota.getAddressCount());

         assertEquals(2, quota.getAddressCount(), "Expected count to be 2 after creating addr3");

      } finally {
         server.stop();
      }
   }

   @Test
   public void testCreateRemoveRecreateSpecific() throws Exception {
      Configuration config = createDefaultConfig(false);

      ResourceQuotaConfig quotaConfig = new ResourceQuotaConfig("test-quota");
      quotaConfig.setMaxAddresses(10);
      config.addResourceQuotaConfig("test-quota", quotaConfig);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");
      config.addAddressSetting("test.#", settings);

      ActiveMQServer server = createServer(false, config);
      server.start();

      try {
         // Create addr1
         System.out.println("=== Creating addr1 ===");
         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr1"), RoutingType.ANYCAST));

         ResourceQuota quota = server.getResourceQuotaService().lookupQuota(SimpleString.of("test.addr1"));
         assertNotNull(quota);
         assertEquals(1, quota.getAddressCount());
         assertNotNull(server.getAddressInfo(SimpleString.of("test.addr1")));

         // Remove addr1
         System.out.println("=== Removing addr1 ===");
         server.removeAddressInfo(SimpleString.of("test.addr1"), null);
         assertEquals(0, quota.getAddressCount());

         // Verify addr1 is truly gone
         AddressInfo info = server.getAddressInfo(SimpleString.of("test.addr1"));
         System.out.println("After removal, addr1 info: " + info);

         // Recreate addr1 with same name
         System.out.println("=== Recreating addr1 ===");
         server.addAddressInfo(new AddressInfo(SimpleString.of("test.addr1"), RoutingType.ANYCAST));
         System.out.println("After recreate: count = " + quota.getAddressCount());

         assertEquals(1, quota.getAddressCount(), "Expected count to be 1 after recreating addr1");
         assertNotNull(server.getAddressInfo(SimpleString.of("test.addr1")));

      } finally {
         server.stop();
      }
   }
}
