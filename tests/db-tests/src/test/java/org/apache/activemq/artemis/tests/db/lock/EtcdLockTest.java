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
package org.apache.activemq.artemis.tests.db.lock;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.server.etcd.EtcdDistributedLock;
import org.apache.activemq.artemis.utils.Wait;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test is disabled by default and only runs when the etcd.load system property is set to true.
 * To enable this test:
 * <ol>
 * <li>Start the etcd server using the script located at {@code tests/db-tests/scripts/start-etcd.sh}</li>
 * <li>Activate the DB-etc-tests Maven profile:
 * <pre>
 * mvn test -P DB-etc-tests
 * </pre>
 * </li>
 * </ol>
 */
public class EtcdLockTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String ETCD_ENDPOINTS = "http://localhost:2379";
   private static final String LOCK_NAME = "/artemis/test/lock";
   private static final int LEASE_TTL_SECONDS = 1;
   private static final long KEEP_ALIVE_INTERVAL_MS = 200;
   private static final int NUM_THREADS = 10;

   private ScheduledExecutorService scheduledExecutor;
   private AtomicInteger lockHolderCount;

   @BeforeEach
   public void setUp() {
      scheduledExecutor = Executors.newScheduledThreadPool(NUM_THREADS);
      lockHolderCount = new AtomicInteger(0);
   }

   @AfterEach
   public void tearDown() {
      scheduledExecutor.shutdownNow();
   }

   @Test
   public void testOnlyOneLockHolderAtATime() throws Exception {
      List<EtcdDistributedLock> locks = getEtcdDistributedLocks();
      try {

         // Start all locks
         for (EtcdDistributedLock lock : locks) {
            lock.start();
         }

         // Wait for exactly one lock to be acquired
         Wait.assertEquals(1, () -> lockHolderCount.get(), 5000, 100);

         logger.info("Stopping ********************************************************************************");

         // Stop all lockAcceps
         for (EtcdDistributedLock lock : locks) {
            lock.stop();
         }

         // Verify that no locks are held after stopping
         Wait.assertEquals(0, () -> lockHolderCount.get(), 5000, 100);
      } finally {
         try {
            locks.forEach(EtcdDistributedLock::stop);
         } catch (Throwable ignored) {
         }
      }
   }

   private @NonNull List<EtcdDistributedLock> getEtcdDistributedLocks() {
      List<EtcdDistributedLock> locks = new ArrayList<>();
      // Create 10 lock instances competing for the same lock
      for (int i = 0; i < NUM_THREADS; i++) {
         EtcdDistributedLock lock = new EtcdDistributedLock(
            ETCD_ENDPOINTS,
            LOCK_NAME,
            LEASE_TTL_SECONDS,
            KEEP_ALIVE_INTERVAL_MS,
            scheduledExecutor,
            () -> {
               logger.info("lock acquired");
               lockHolderCount.incrementAndGet();
            },
            () -> {
               logger.info("lock released");
               lockHolderCount.decrementAndGet();
            }
         );
         locks.add(lock);
      }
      return locks;
   }
}
