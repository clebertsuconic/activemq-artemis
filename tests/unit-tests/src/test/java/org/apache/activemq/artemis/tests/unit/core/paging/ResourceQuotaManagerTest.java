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
package org.apache.activemq.artemis.tests.unit.core.paging;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.paging.ResourceQuotaManager;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ResourceQuotaManagerTest {

   private ResourceQuotaManager manager;
   private HierarchicalObjectRepository<ResourceQuota> repository;
   private WildcardConfiguration wildcardConfig;

   @BeforeEach
   public void setUp() {
      wildcardConfig = new WildcardConfiguration();
      repository = new HierarchicalObjectRepository<>(wildcardConfig);
      manager = new ResourceQuotaManager(repository, wildcardConfig);
   }

   @Test
   public void testBasicQuotaLookup() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxMessageBytes(10000L);
      manager.addQuota("test-quota", quota);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("test-quota");

      ResourceQuota found = manager.getQuotaForAddress(SimpleString.of("test.address"), settings);
      assertNotNull(found);
      assertEquals("test-quota", found.getName());
      assertEquals(10000L, found.getMaxMessageBytes());
   }

   @Test
   public void testQuotaNotFound() {
      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("non-existent");

      ResourceQuota found = manager.getQuotaForAddress(SimpleString.of("test.address"), settings);
      assertNull(found);
   }

   @Test
   public void testNullSettings() {
      ResourceQuota found = manager.getQuotaForAddress(SimpleString.of("test.address"), null);
      assertNull(found);
   }

   @Test
   public void testSettingsWithNoQuota() {
      AddressSettings settings = new AddressSettings();
      ResourceQuota found = manager.getQuotaForAddress(SimpleString.of("test.address"), settings);
      assertNull(found);
   }

   @Test
   public void testEstablishSimpleParentRelationship() {
      ResourceQuota parent = new ResourceQuota("parent");
      ResourceQuota child = new ResourceQuota("child");
      child.setPartOf("parent");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("parent", parent);
      allQuotas.put("child", child);

      manager.establishParentRelationships(allQuotas);

      assertNotNull(child.getParent());
      assertEquals("parent", child.getParent().getName());
   }

   @Test
   public void testEstablishThreeLevelHierarchy() {
      ResourceQuota global = new ResourceQuota("global");

      ResourceQuota region = new ResourceQuota("EU");
      region.setPartOf("global");

      ResourceQuota country = new ResourceQuota("EU.fr");
      country.setPartOf("EU");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("global", global);
      allQuotas.put("EU", region);
      allQuotas.put("EU.fr", country);

      manager.establishParentRelationships(allQuotas);

      assertNull(global.getParent());
      assertNotNull(region.getParent());
      assertEquals("global", region.getParent().getName());
      assertNotNull(country.getParent());
      assertEquals("EU", country.getParent().getName());
      assertNotNull(country.getParent().getParent());
      assertEquals("global", country.getParent().getParent().getName());
   }

   @Test
   public void testCircularReferenceDetection() {
      ResourceQuota quota1 = new ResourceQuota("quota1");
      quota1.setPartOf("quota2");

      ResourceQuota quota2 = new ResourceQuota("quota2");
      quota2.setPartOf("quota1");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("quota1", quota1);
      allQuotas.put("quota2", quota2);

      // Should not throw, just log error
      manager.establishParentRelationships(allQuotas);

      // Neither should have parent set due to circular reference
      assertNull(quota1.getParent());
      assertNull(quota2.getParent());
   }

   @Test
   public void testMissingParent() {
      ResourceQuota child = new ResourceQuota("child");
      child.setPartOf("non-existent-parent");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("child", child);

      manager.establishParentRelationships(allQuotas);

      assertNull(child.getParent());
   }

   @Test
   public void testQuotaWithoutParent() {
      ResourceQuota quota = new ResourceQuota("standalone");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("standalone", quota);

      manager.establishParentRelationships(allQuotas);

      assertNull(quota.getParent());
   }

   @Test
   public void testBytesPropagateUpHierarchy() {
      ResourceQuota global = new ResourceQuota("global");
      global.setMaxMessageBytes(100000L);

      ResourceQuota region = new ResourceQuota("EU");
      region.setMaxMessageBytes(50000L);
      region.setPartOf("global");

      ResourceQuota country = new ResourceQuota("EU.fr");
      country.setMaxMessageBytes(10000L);
      country.setPartOf("EU");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("global", global);
      allQuotas.put("EU", region);
      allQuotas.put("EU.fr", country);

      manager.establishParentRelationships(allQuotas);

      // Add bytes at country level
      country.addSize(5000, false);

      assertEquals(5000, country.getSize());
      assertEquals(5000, region.getSize());
      assertEquals(5000, global.getSize());
   }

   @Test
   public void testCountsPropagateUpHierarchy() {
      ResourceQuota parent = new ResourceQuota("parent");
      parent.setMaxAddresses(100);
      parent.setMaxQueues(100);

      ResourceQuota child = new ResourceQuota("child");
      child.setMaxAddresses(50);
      child.setMaxQueues(50);
      child.setPartOf("parent");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("parent", parent);
      allQuotas.put("child", child);

      manager.establishParentRelationships(allQuotas);

      child.incrementAddressCount();
      child.incrementQueueCount();

      assertEquals(1, child.getAddressCount());
      assertEquals(1, child.getQueueCount());
      assertEquals(1, parent.getAddressCount());
      assertEquals(1, parent.getQueueCount());
   }

   @Test
   public void testGetAllQuotas() {
      ResourceQuota quota1 = new ResourceQuota("quota1");
      ResourceQuota quota2 = new ResourceQuota("quota2");

      manager.addQuota("quota1", quota1);
      manager.addQuota("quota2", quota2);

      Map<String, ResourceQuota> all = manager.getAllQuotas();
      assertNotNull(all);
      // Note: getAllQuotas returns instantiated quotas, not base quotas
      // This is a limitation for Phase 1
   }

   @Test
   public void testClearInstances() {
      manager.clearInstances();

      Map<String, ResourceQuota> instances = manager.getInstantiatedQuotas();
      assertEquals(0, instances.size());
   }

   @Test
   public void testWildcardQuotaTemplatePhase1() {
      // In Phase 1, wildcard templates are stored but not yet instantiated
      ResourceQuota template = new ResourceQuota("EU.*");
      template.setMaxMessageBytes(5000L);
      manager.addQuota("EU.*", template);

      AddressSettings settings = new AddressSettings();
      settings.setResourceQuota("EU.*");

      // For Phase 1, this returns the template itself
      ResourceQuota found = manager.getQuotaForAddress(SimpleString.of("eu.fr.test"), settings);
      assertNotNull(found);
      assertEquals("EU.*", found.getName());
   }

   @Test
   public void testMultipleQuotasWithHierarchy() {
      ResourceQuota global = new ResourceQuota("global");
      global.setMaxMessageBytes(1000000L);

      ResourceQuota us = new ResourceQuota("US");
      us.setMaxMessageBytes(400000L);
      us.setPartOf("global");

      ResourceQuota eu = new ResourceQuota("EU");
      eu.setMaxMessageBytes(400000L);
      eu.setPartOf("global");

      ResourceQuota usCa = new ResourceQuota("US.ca");
      usCa.setMaxMessageBytes(100000L);
      usCa.setPartOf("US");

      ResourceQuota euFr = new ResourceQuota("EU.fr");
      euFr.setMaxMessageBytes(100000L);
      euFr.setPartOf("EU");

      Map<String, ResourceQuota> allQuotas = new HashMap<>();
      allQuotas.put("global", global);
      allQuotas.put("US", us);
      allQuotas.put("EU", eu);
      allQuotas.put("US.ca", usCa);
      allQuotas.put("EU.fr", euFr);

      manager.establishParentRelationships(allQuotas);

      // US.ca should propagate to US and global
      usCa.addSize(50000, false);
      assertEquals(50000, usCa.getSize());
      assertEquals(50000, us.getSize());
      assertEquals(50000, global.getSize());
      assertEquals(0, eu.getSize());
      assertEquals(0, euFr.getSize());

      // EU.fr should propagate to EU and global
      euFr.addSize(30000, false);
      assertEquals(30000, euFr.getSize());
      assertEquals(30000, eu.getSize());
      assertEquals(80000, global.getSize());
      assertEquals(50000, us.getSize());
      assertEquals(50000, usCa.getSize());
   }
}
