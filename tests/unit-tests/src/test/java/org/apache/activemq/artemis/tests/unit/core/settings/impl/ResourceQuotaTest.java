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
package org.apache.activemq.artemis.tests.unit.core.settings.impl;

import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResourceQuotaTest {

   @Test
   public void testBasicQuotaCreation() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      assertEquals("test-quota", quota.getName());
      assertEquals(ResourceQuota.DEFAULT_MAX_MESSAGE_BYTES, quota.getMaxMessageBytes());
      assertEquals(ResourceQuota.DEFAULT_MAX_ADDRESSES, quota.getMaxAddresses());
      assertEquals(ResourceQuota.DEFAULT_MAX_QUEUES, quota.getMaxQueues());
      assertNull(quota.getPartOf());
   }

   @Test
   public void testQuotaConfiguration() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxMessageBytes(1024L * 1024L);
      quota.setMaxAddresses(100);
      quota.setMaxQueues(50);
      quota.setPartOf("parent-quota");

      assertEquals(1024L * 1024L, quota.getMaxMessageBytes());
      assertEquals(100, quota.getMaxAddresses());
      assertEquals(50, quota.getMaxQueues());
      assertEquals("parent-quota", quota.getPartOf());
   }

   @Test
   public void testByteTracking() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxMessageBytes(1000L);

      assertEquals(0, quota.getSize());

      quota.addSize(100, false);
      assertEquals(100, quota.getSize());
      assertEquals(1, quota.getElements());

      quota.addSize(200, false);
      assertEquals(300, quota.getSize());
      assertEquals(2, quota.getElements());

      quota.addSize(-50, false);
      assertEquals(250, quota.getSize());
      assertEquals(1, quota.getElements());
   }

   @Test
   public void testByteLimitExceeded() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxMessageBytes(1000L);

      assertFalse(quota.isOverByteLimit());
      assertFalse(quota.isOverLimit());

      quota.addSize(500, false);
      assertFalse(quota.isOverByteLimit());

      quota.addSize(600, false);
      assertTrue(quota.isOverByteLimit());
      assertTrue(quota.isOverLimit());

      // Going back under the lower mark (90% of max = 900)
      quota.addSize(-300, false);
      assertFalse(quota.isOverByteLimit());
   }

   @Test
   public void testAddressCountTracking() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxAddresses(5);

      assertEquals(0, quota.getAddressCount());
      assertFalse(quota.isOverAddressLimit());

      quota.incrementAddressCount();
      assertEquals(1, quota.getAddressCount());
      assertFalse(quota.isOverAddressLimit());

      for (int i = 0; i < 5; i++) {
         quota.incrementAddressCount();
      }
      assertEquals(6, quota.getAddressCount());
      assertTrue(quota.isOverAddressLimit());
      assertTrue(quota.isOverLimit());

      quota.decrementAddressCount();
      assertEquals(5, quota.getAddressCount());
      assertFalse(quota.isOverAddressLimit());
   }

   @Test
   public void testQueueCountTracking() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxQueues(10);

      assertEquals(0, quota.getQueueCount());
      assertFalse(quota.isOverQueueLimit());

      for (int i = 0; i < 10; i++) {
         quota.incrementQueueCount();
      }
      assertEquals(10, quota.getQueueCount());
      assertFalse(quota.isOverQueueLimit());

      quota.incrementQueueCount();
      assertEquals(11, quota.getQueueCount());
      assertTrue(quota.isOverQueueLimit());
      assertTrue(quota.isOverLimit());

      quota.decrementQueueCount();
      assertEquals(10, quota.getQueueCount());
      assertFalse(quota.isOverQueueLimit());
   }

   @Test
   public void testParentPropagation() {
      ResourceQuota parent = new ResourceQuota("parent");
      parent.setMaxMessageBytes(10000L);
      parent.setMaxAddresses(100);
      parent.setMaxQueues(100);

      ResourceQuota child = new ResourceQuota("child");
      child.setMaxMessageBytes(5000L);
      child.setMaxAddresses(50);
      child.setMaxQueues(50);
      child.setParent(parent);

      // Test byte propagation
      child.addSize(1000, false);
      assertEquals(1000, child.getSize());
      assertEquals(1000, parent.getSize());

      // Test address count propagation
      child.incrementAddressCount();
      assertEquals(1, child.getAddressCount());
      assertEquals(1, parent.getAddressCount());

      // Test queue count propagation
      child.incrementQueueCount();
      assertEquals(1, child.getQueueCount());
      assertEquals(1, parent.getQueueCount());

      // Test decrement propagation
      child.decrementAddressCount();
      assertEquals(0, child.getAddressCount());
      assertEquals(0, parent.getAddressCount());

      child.decrementQueueCount();
      assertEquals(0, child.getQueueCount());
      assertEquals(0, parent.getQueueCount());
   }

   @Test
   public void testThreeLevelHierarchy() {
      ResourceQuota global = new ResourceQuota("global");
      global.setMaxMessageBytes(100000L);

      ResourceQuota region = new ResourceQuota("EU");
      region.setMaxMessageBytes(50000L);
      region.setParent(global);

      ResourceQuota country = new ResourceQuota("EU.fr");
      country.setMaxMessageBytes(10000L);
      country.setParent(region);

      // Add bytes to country-level quota
      country.addSize(5000, false);

      assertEquals(5000, country.getSize());
      assertEquals(5000, region.getSize());
      assertEquals(5000, global.getSize());

      // Add more to exceed country limit but not region
      country.addSize(6000, false);
      assertEquals(11000, country.getSize());
      assertTrue(country.isOverByteLimit());
      assertFalse(region.isOverByteLimit());
      assertFalse(global.isOverByteLimit());
   }

   @Test
   public void testOverCallback() throws Exception {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxMessageBytes(1000L);

      CountDownLatch latch = new CountDownLatch(1);
      quota.setOverCallback(latch::countDown);

      quota.addSize(500, false);
      assertEquals(1, latch.getCount());

      quota.addSize(600, false);
      assertTrue(latch.await(1, TimeUnit.SECONDS));
   }

   @Test
   public void testUnderCallback() throws Exception {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxMessageBytes(1000L);

      // First go over the limit
      quota.addSize(1100, false);
      assertTrue(quota.isOverByteLimit());

      CountDownLatch latch = new CountDownLatch(1);
      quota.setUnderCallback(latch::countDown);

      // Now go back under the lower mark (900)
      quota.addSize(-300, false);
      assertTrue(latch.await(1, TimeUnit.SECONDS));
      assertFalse(quota.isOverByteLimit());
   }

   @Test
   public void testThreadSafety() throws Exception {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxMessageBytes(1000000L);
      quota.setMaxAddresses(1000);
      quota.setMaxQueues(1000);

      int threadCount = 10;
      int operationsPerThread = 100;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(threadCount);

      for (int i = 0; i < threadCount; i++) {
         new Thread(() -> {
            try {
               startLatch.await();
               for (int j = 0; j < operationsPerThread; j++) {
                  quota.addSize(10, false);
                  quota.incrementAddressCount();
                  quota.incrementQueueCount();
               }
            } catch (Exception e) {
               e.printStackTrace();
            } finally {
               doneLatch.countDown();
            }
         }).start();
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

      assertEquals(threadCount * operationsPerThread * 10, quota.getSize());
      assertEquals(threadCount * operationsPerThread, quota.getAddressCount());
      assertEquals(threadCount * operationsPerThread, quota.getQueueCount());
   }

   @Test
   public void testNegativeCountProtection() {
      ResourceQuota quota = new ResourceQuota("test");

      // Decrement address count when it's already 0
      quota.decrementAddressCount();
      assertEquals(0, quota.getAddressCount());

      // Decrement queue count when it's already 0
      quota.decrementQueueCount();
      assertEquals(0, quota.getQueueCount());
   }

   @Test
   public void testSizeOnlyTracking() {
      ResourceQuota quota = new ResourceQuota("test");

      quota.addSize(100, true);
      assertEquals(100, quota.getSize());
      assertEquals(0, quota.getElements());

      quota.addSize(50, false);
      assertEquals(150, quota.getSize());
      assertEquals(1, quota.getElements());
   }

   // ========== Tests for Atomic Operations (Bug Fix #2) ==========

   @Test
   public void testTryIncrementAddressCountSuccess() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxAddresses(5);

      // Should succeed - within limit
      assertTrue(quota.tryIncrementAddressCount());
      assertEquals(1, quota.getAddressCount());

      assertTrue(quota.tryIncrementAddressCount());
      assertEquals(2, quota.getAddressCount());
   }

   @Test
   public void testTryIncrementAddressCountFailsAtLimit() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxAddresses(3);

      // Increment to limit
      assertTrue(quota.tryIncrementAddressCount());
      assertTrue(quota.tryIncrementAddressCount());
      assertTrue(quota.tryIncrementAddressCount());
      assertEquals(3, quota.getAddressCount());

      // Should fail - at limit
      assertFalse(quota.tryIncrementAddressCount());
      assertEquals(3, quota.getAddressCount());

      // Still at limit
      assertFalse(quota.tryIncrementAddressCount());
      assertEquals(3, quota.getAddressCount());
   }

   @Test
   public void testTryIncrementQueueCountSuccess() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxQueues(10);

      // Should succeed - within limit
      assertTrue(quota.tryIncrementQueueCount());
      assertEquals(1, quota.getQueueCount());

      assertTrue(quota.tryIncrementQueueCount());
      assertEquals(2, quota.getQueueCount());
   }

   @Test
   public void testTryIncrementQueueCountFailsAtLimit() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxQueues(2);

      // Increment to limit
      assertTrue(quota.tryIncrementQueueCount());
      assertTrue(quota.tryIncrementQueueCount());
      assertEquals(2, quota.getQueueCount());

      // Should fail - at limit
      assertFalse(quota.tryIncrementQueueCount());
      assertEquals(2, quota.getQueueCount());
   }

   @Test
   public void testTryIncrementWithParentQuota() {
      ResourceQuota parent = new ResourceQuota("parent");
      parent.setMaxAddresses(5);

      ResourceQuota child = new ResourceQuota("child");
      child.setMaxAddresses(3);
      child.setParent(parent);

      // Increment child - should propagate to parent
      assertTrue(child.tryIncrementAddressCount());
      assertEquals(1, child.getAddressCount());
      assertEquals(1, parent.getAddressCount());

      assertTrue(child.tryIncrementAddressCount());
      assertEquals(2, child.getAddressCount());
      assertEquals(2, parent.getAddressCount());
   }

   @Test
   public void testTryIncrementFailsWhenParentAtLimit() {
      ResourceQuota parent = new ResourceQuota("parent");
      parent.setMaxAddresses(3);

      ResourceQuota child = new ResourceQuota("child");
      child.setMaxAddresses(10); // Child has higher limit but parent restricts
      child.setParent(parent);

      // Fill parent to limit
      assertTrue(child.tryIncrementAddressCount());
      assertTrue(child.tryIncrementAddressCount());
      assertTrue(child.tryIncrementAddressCount());
      assertEquals(3, child.getAddressCount());
      assertEquals(3, parent.getAddressCount());

      // Should fail because parent is at limit
      assertFalse(child.tryIncrementAddressCount());
      assertEquals(3, child.getAddressCount());
      assertEquals(3, parent.getAddressCount());
   }

   @Test
   public void testTryIncrementWithNoLimit() {
      ResourceQuota quota = new ResourceQuota("test-quota");
      // No limit set (maxAddresses = -1 by default)

      // Should always succeed when no limit
      for (int i = 0; i < 100; i++) {
         assertTrue(quota.tryIncrementAddressCount());
      }
      assertEquals(100, quota.getAddressCount());
   }

   @Test
   public void testConcurrentTryIncrementAddressCount() throws Exception {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxAddresses(100);

      int numThreads = 10;
      int incrementsPerThread = 20;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);
      AtomicInteger successCount = new AtomicInteger(0);

      for (int i = 0; i < numThreads; i++) {
         new Thread(() -> {
            try {
               startLatch.await();
               for (int j = 0; j < incrementsPerThread; j++) {
                  if (quota.tryIncrementAddressCount()) {
                     successCount.incrementAndGet();
                  }
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } finally {
               doneLatch.countDown();
            }
         }).start();
      }

      startLatch.countDown(); // Start all threads
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

      // Exactly 100 should succeed (the limit)
      assertEquals(100, successCount.get());
      assertEquals(100, quota.getAddressCount());
   }

   @Test
   public void testConcurrentTryIncrementQueueCount() throws Exception {
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxQueues(50);

      int numThreads = 5;
      int incrementsPerThread = 20;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);
      AtomicInteger successCount = new AtomicInteger(0);

      for (int i = 0; i < numThreads; i++) {
         new Thread(() -> {
            try {
               startLatch.await();
               for (int j = 0; j < incrementsPerThread; j++) {
                  if (quota.tryIncrementQueueCount()) {
                     successCount.incrementAndGet();
                  }
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } finally {
               doneLatch.countDown();
            }
         }).start();
      }

      startLatch.countDown(); // Start all threads
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

      // Exactly 50 should succeed (the limit)
      assertEquals(50, successCount.get());
      assertEquals(50, quota.getQueueCount());
   }

   @Test
   public void testConcurrentTryIncrementWithParentHierarchy() throws Exception {
      ResourceQuota parent = new ResourceQuota("parent");
      parent.setMaxAddresses(30);

      ResourceQuota child = new ResourceQuota("child");
      child.setMaxAddresses(50); // Higher than parent
      child.setParent(parent);

      int numThreads = 5;
      int incrementsPerThread = 10;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);
      AtomicInteger successCount = new AtomicInteger(0);

      for (int i = 0; i < numThreads; i++) {
         new Thread(() -> {
            try {
               startLatch.await();
               for (int j = 0; j < incrementsPerThread; j++) {
                  if (child.tryIncrementAddressCount()) {
                     successCount.incrementAndGet();
                  }
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } finally {
               doneLatch.countDown();
            }
         }).start();
      }

      startLatch.countDown(); // Start all threads
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

      // Should be limited by parent's limit of 30
      assertEquals(30, successCount.get());
      assertEquals(30, child.getAddressCount());
      assertEquals(30, parent.getAddressCount());
   }

   @Test
   public void testTryIncrementDoesNotLeakOnParentFailure() {
      ResourceQuota parent = new ResourceQuota("parent");
      parent.setMaxAddresses(2);

      ResourceQuota child = new ResourceQuota("child");
      child.setMaxAddresses(10);
      child.setParent(parent);

      // Fill parent to limit
      assertTrue(child.tryIncrementAddressCount());
      assertTrue(child.tryIncrementAddressCount());
      assertEquals(2, parent.getAddressCount());

      // Try to increment child - should fail and NOT leak parent count
      assertFalse(child.tryIncrementAddressCount());
      assertEquals(2, child.getAddressCount());
      assertEquals(2, parent.getAddressCount()); // Parent should still be 2, not 3
   }

   @Test
   public void testAtomicIncrementVsNonAtomicBehavior() throws Exception {
      // This test demonstrates that tryIncrement is atomic while increment+check is not
      ResourceQuota quota = new ResourceQuota("test-quota");
      quota.setMaxAddresses(10);

      int numThreads = 20;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);
      AtomicInteger successfulAtomicIncrements = new AtomicInteger(0);

      for (int i = 0; i < numThreads; i++) {
         new Thread(() -> {
            try {
               startLatch.await();
               if (quota.tryIncrementAddressCount()) {
                  successfulAtomicIncrements.incrementAndGet();
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } finally {
               doneLatch.countDown();
            }
         }).start();
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

      // With atomic tryIncrement, exactly 10 threads should succeed
      assertEquals(10, successfulAtomicIncrements.get());
      assertEquals(10, quota.getAddressCount());
      // Without atomic operation, count could exceed 10 due to race conditions
   }
}
