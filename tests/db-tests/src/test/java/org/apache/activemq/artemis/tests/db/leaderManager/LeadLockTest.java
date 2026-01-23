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
package org.apache.activemq.artemis.tests.db.leaderManager;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.activemq.artemis.core.server.leader.LeaderManager;
import org.apache.activemq.artemis.lockmanager.DistributedLockManager;
import org.apache.activemq.artemis.lockmanager.DistributedLockManagerFactory;
import org.apache.activemq.artemis.lockmanager.MutableLong;
import org.apache.activemq.artemis.lockmanager.Registry;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test needs external dependencies. It follows the same pattern described at {@link DualMirrorSingleAcceptorRunningTest}.
 * please read the documentation from that test for more detail on how to run this test.
 */
public class LeadLockTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String ZK_ENDPOINTS = "localhost:2181";
   private static final long KEEP_ALIVE_INTERVAL_MS = 200;
   private static final int NUM_THREADS = 10;

   private ExecutorService executorService;
   private ScheduledExecutorService scheduledExecutor;
   private AtomicInteger lockHolderCount;
   private AtomicInteger lockChanged;
   private OrderedExecutorFactory executorFactory;

   @BeforeEach
   @Override
   public void setUp() {
      disableCheckThread();
      scheduledExecutor = Executors.newScheduledThreadPool(NUM_THREADS);
      executorService = Executors.newFixedThreadPool(NUM_THREADS * 2);
      executorFactory = new OrderedExecutorFactory(executorService);
      lockHolderCount = new AtomicInteger(0);
      lockChanged = new AtomicInteger(0);
   }

   @AfterEach
   @Override
   public void tearDown() {
      scheduledExecutor.shutdownNow();
      executorService.shutdownNow();
   }

   @Test
   public void testWithFile() throws Exception {
      internalTest(i -> getFileLeaders(i));
   }

   @EnabledIfSystemProperty(named = "ZK.load", matches = "true")
   @Test
   public void testWithZK() throws Exception {
      internalTest(i -> getZKLeaders(i));
   }

   private void internalTest(Function<Integer, List<LeaderManager>> leaderManagerSupplier) throws Exception {
      testOnlyOneLockHolderAtATime(leaderManagerSupplier.apply(NUM_THREADS));
      testAddAfterLocked(leaderManagerSupplier.apply(1).get(0));
      testRetryAfterError(leaderManagerSupplier.apply(1).get(0));
      testRetryAfterErrorWithDelayAdd(leaderManagerSupplier.apply(1).get(0));

      {
         List<LeaderManager> list = leaderManagerSupplier.apply(2);
         testNoRetryWhileNotAcquired(list.get(0), list.get(1));
      }
   }

   private void testAddAfterLocked(LeaderManager leaderManager) throws Exception {
      lockHolderCount.set(0);
      lockChanged.set(0);

      try {
         leaderManager.start();
         Wait.assertEquals(1, () -> lockHolderCount.get(), 15000, 100);

         AtomicInteger afterRunning = new AtomicInteger(0);
         assertTrue(leaderManager.isLocked());
         leaderManager.addLeadAcquired(afterRunning::incrementAndGet);

         Wait.assertEquals(1, afterRunning::get);

         assertEquals(1, lockHolderCount.get());
      } finally {
         leaderManager.stop();
      }
   }

   private void testRetryAfterError(LeaderManager leaderManager) throws Exception {
      lockHolderCount.set(0);
      lockChanged.set(0);

      AtomicBoolean succeeded = new AtomicBoolean(false);
      AtomicInteger numberOfTries = new AtomicInteger(0);
      try {
         leaderManager.addLeadAcquired(() -> {
            if (numberOfTries.incrementAndGet() < 5) {
               throw new IOException("please retry");
            }
            succeeded.set(true);
         });
         leaderManager.start();

         Wait.assertTrue(succeeded::get, 5000, 100);
         Wait.assertEquals(1, lockHolderCount::get);
      } finally {
         leaderManager.stop();
      }
   }

   private void testRetryAfterErrorWithDelayAdd(LeaderManager leaderManager) throws Exception {
      lockHolderCount.set(0);
      lockChanged.set(0);

      AtomicBoolean succeeded = new AtomicBoolean(false);
      AtomicInteger numberOfTries = new AtomicInteger(0);
      try {
         leaderManager.start();
         Wait.assertEquals(1, lockHolderCount::get);

         leaderManager.addLeadAcquired(() -> {
            if (numberOfTries.incrementAndGet() < 5) {
               throw new RuntimeException("please retry");
            }
            succeeded.set(true);
         });

         Wait.assertTrue(succeeded::get, 5000, 100);
         Wait.assertEquals(1, lockHolderCount::get);
      } finally {
         leaderManager.stop();
      }
   }

   // validate that no retry would happen since the lock wasn't held in the secondLock
   private void testNoRetryWhileNotAcquired(LeaderManager firstLock, LeaderManager secondLock) throws Exception {
      lockHolderCount.set(0);
      lockChanged.set(0);
      AtomicBoolean throwError = new AtomicBoolean(true);
      AtomicBoolean errorHappened = new AtomicBoolean(false);

      AtomicBoolean succeeded = new AtomicBoolean(false);
      try {
         firstLock.start();
         Wait.assertEquals(1, lockHolderCount::get);
         assertTrue(firstLock.isLocked());
         secondLock.start();
         assertFalse(secondLock.isLocked());

         secondLock.addLeadAcquired(() -> {
            if (throwError.get()) {
               errorHappened.set(true);
               throw new RuntimeException("please retry");
            }
            succeeded.set(true);
         });

         assertFalse(succeeded.get());
         assertFalse(errorHappened.get());
         firstLock.stop();
         Wait.assertTrue(errorHappened::get, 5000, 100);
         throwError.set(false);
         Wait.assertTrue(succeeded::get, 5000, 100);
         Wait.assertEquals(1, lockHolderCount::get);
      } finally {
         firstLock.stop();
         secondLock.stop();
      }
   }


   private void testOnlyOneLockHolderAtATime(List<LeaderManager> leaderManagers) throws Exception {
      try {

         leaderManagers.forEach(LeaderManager::start);

         Wait.assertEquals(1, () -> lockHolderCount.get(), 15000, 100);

         long value = RandomUtil.randomPositiveLong();

         boolean first = true;

         for (LeaderManager leaderManager : leaderManagers) {
            MutableLong mutableLong = leaderManager.getLockManager().getMutableLong("mutableLong");
            if (first) {
               mutableLong.set(value);
               first = false;
            } else {
               assertEquals(value, mutableLong.get());
            }
            mutableLong.close();
         }

         logger.info("Stopping ********************************************************************************");

         // We keep stopping lockManager that is holding the lock
         // we do this until we stop every one of the locks
         while (!leaderManagers.isEmpty()) {
            if (!Wait.waitFor(() -> lockHolderCount.get() == 1, 15000, 100)) {
               for (LeaderManager lock : leaderManagers) {
                  logger.info("lock {} is holdingLock={}", lock.getDebugInfo(), lock.isLocked());
               }
            }
            Wait.assertEquals(1, () -> lockHolderCount.get(), 15000, 100);
            for (LeaderManager lock : leaderManagers) {
               if (lock.isLocked()) {
                  long changed = lockChanged.get();
                  lock.stop();
                  leaderManagers.remove(lock);
                  //Wait.assertTrue(() -> lockChanged.get() != changed, 5000, 100);
                  break;
               }
            }
         }

         // Verify that no locks are held after stopping
         Wait.assertEquals(0, () -> lockHolderCount.get(), 15000, 100);
      } finally {
         try {
            leaderManagers.forEach(LeaderManager::stop);
         } catch (Throwable ignored) {
         }
      }
   }

   private List<LeaderManager> getFileLeaders(int numberOfLeaders) {
      File file = new File(getTemporaryDir() + "/lockFolder");
      file.mkdirs();
      HashMap<String, String> parameters = new HashMap<>();
      parameters.put("locks-folder", file.getAbsolutePath());
      return getLeaders(numberOfLeaders, "file", parameters);
   }

   private List<LeaderManager> getZKLeaders(int numberOfLeaders) {
      HashMap<String, String> parameters = new HashMap<>();
      parameters.put("connect-string", ZK_ENDPOINTS);
      return getLeaders(numberOfLeaders, "ZK", parameters);
   }

   private List<LeaderManager> getLeaders(int numberOfLeaders, String factoryName, HashMap<String, String> parameters) {
      return getLeaders(numberOfLeaders, () -> {
         DistributedLockManagerFactory factory = Registry.getInstance().getFactory(factoryName);
         return factory.build(parameters);
      });
   }

   private List<LeaderManager> getLeaders(int numberOfLeaders, Supplier<DistributedLockManager> lockManagerSupplier) {
      List<LeaderManager> locks = new ArrayList<>();
      String lockName = "lock-test-" + RandomUtil.randomUUIDString();
      for (int i = 0; i < numberOfLeaders; i++) {
         DistributedLockManager lockManager = lockManagerSupplier.get();

         LeaderManager leaderManager = new LeaderManager(scheduledExecutor, executorFactory.getExecutor(), KEEP_ALIVE_INTERVAL_MS, lockManager, lockName, lockName);
         leaderManager.addLeadAcquired(() -> lock(leaderManager));
         leaderManager.addLeadReleased(() -> unlock(leaderManager));
         leaderManager.addLeadReleased(() -> lockChanged.incrementAndGet());
         leaderManager.addLeadAcquired(() -> lockChanged.incrementAndGet());
         leaderManager.setDebugInfo("ID" + i);
         locks.add(leaderManager);
      }
      return locks;
   }

   private void lock(LeaderManager leaderManager) {
      logger.info("++Leader {} lock", leaderManager.getDebugInfo());
      lockHolderCount.incrementAndGet();
   }

   private void unlock(LeaderManager leaderManager) {
      logger.info("--Leader {} unlocking", leaderManager.getDebugInfo());
      lockHolderCount.decrementAndGet();
   }

}
