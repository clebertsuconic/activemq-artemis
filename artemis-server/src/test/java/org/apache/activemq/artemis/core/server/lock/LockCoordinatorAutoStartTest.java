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
package org.apache.activemq.artemis.core.server.lock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.LockCoordinatorConfiguration;
import org.apache.activemq.artemis.lockmanager.DistributedLock;
import org.apache.activemq.artemis.lockmanager.DistributedLockManager;
import org.apache.activemq.artemis.lockmanager.MutableLong;
import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.junit.jupiter.api.Test;

/**
 * Unit-level coverage for the {@code auto-start} plumbing of lock coordinators: the config default/setter and the
 * {@link LockCoordinator} flag itself. Whether a coordinator actually stays stopped until the broker is activated
 * (or until started through management) is exercised by integration tests, since it depends on the server's
 * activation lifecycle.
 */
public class LockCoordinatorAutoStartTest extends ArtemisTestCase {

   private static final int CHECK_PERIOD = 60_000;

   @Test
   public void testLockCoordinatorConfigurationAutoStartDefaultsToTrue() {
      LockCoordinatorConfiguration configuration = new LockCoordinatorConfiguration();
      assertTrue(configuration.isAutoStart(), "auto-start must default to true");
   }

   @Test
   public void testLockCoordinatorConfigurationAutoStartSetter() {
      LockCoordinatorConfiguration configuration = new LockCoordinatorConfiguration();
      LockCoordinatorConfiguration returned = configuration.setAutoStart(false);
      assertSame(configuration, returned, "setAutoStart must return this for fluent chaining");
      assertFalse(configuration.isAutoStart());
   }

   @Test
   public void testLockCoordinatorAutoStartDefaultsToTrue() {
      LockCoordinator coordinator = newCoordinator();
      assertTrue(coordinator.isAutoStart(), "a LockCoordinator must be auto-starting unless configured otherwise");
   }

   @Test
   public void testLockCoordinatorAutoStartSetter() {
      LockCoordinator coordinator = newCoordinator();
      LockCoordinator returned = coordinator.setAutoStart(false);
      assertSame(coordinator, returned, "setAutoStart must return this for fluent chaining");
      assertFalse(coordinator.isAutoStart());
   }

   private LockCoordinator newCoordinator() {
      // scheduledExecutor is only needed once start() is called, which none of these tests do.
      final Executor directExecutor = Runnable::run;
      return new LockCoordinator(null, directExecutor, CHECK_PERIOD, new NoopLockManager(), "theLock", "theLock");
   }

   /**
    * A DistributedLockManager whose methods are never expected to be exercised by these tests: they only check the
    * autoStart flag on the LockCoordinator itself, without ever calling start()/run().
    */
   private static class NoopLockManager implements DistributedLockManager {

      private volatile boolean started;

      @Override
      public void addUnavailableManagerListener(UnavailableManagerListener listener) {
      }

      @Override
      public void removeUnavailableManagerListener(UnavailableManagerListener listener) {
      }

      @Override
      public boolean start(long timeout, TimeUnit unit) {
         started = true;
         return true;
      }

      @Override
      public void start() {
         started = true;
      }

      @Override
      public boolean isStarted() {
         return started;
      }

      @Override
      public void stop() {
         started = false;
      }

      @Override
      public DistributedLock getDistributedLock(String lockId) {
         throw new UnsupportedOperationException("not needed by these tests");
      }

      @Override
      public MutableLong getMutableLong(String mutableLongId) {
         throw new UnsupportedOperationException("not needed by these tests");
      }
   }
}
