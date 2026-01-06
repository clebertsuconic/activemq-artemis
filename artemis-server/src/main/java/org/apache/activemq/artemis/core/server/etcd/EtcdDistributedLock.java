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
package org.apache.activemq.artemis.core.server.etcd;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An exclusive distributed lock implementation using etcd.
 *
 * This lock provides coordination across distributed systems by:
 * - Attempting to acquire an exclusive lock on the first scheduled run
 * - Invoking the onLockAcquired callback when the lock is successfully acquired
 * - Invoking the onLockLost callback if the lock cannot be acquired or is subsequently lost
 * - Maintaining the lock through periodic keep-alive operations until stop()
 *
 * The lock uses etcd's lease mechanism to ensure automatic cleanup if the
 * holder becomes unavailable. Keep-alive operations are performed at the
 * configured interval to maintain the lease and detect lock loss.
 */
public class EtcdDistributedLock extends ActiveMQScheduledComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final Client etcdClient;
   private final Lock lockClient;
   private final Lease leaseClient;
   private final ByteSequence lockKey;
   private final int leaseTTLSeconds;
   private final Runnable onLockAcquired;
   private final Runnable onLockLost;

   private ByteSequence currentLockKey;
   private long currentLeaseId;
   private volatile boolean lockHeld;

   /**
    * Creates a new EtcdDistributedLock.
    *
    * @param etcdEndpoints comma-separated etcd endpoints (e.g., "http://localhost:2379")
    * @param lockName the name/key of the lock
    * @param leaseTTLSeconds lease time-to-live in seconds
    * @param keepAliveIntervalMs keep-alive check interval in milliseconds
    * @param scheduledExecutor executor for scheduling keep-alive tasks
    * @param onLockAcquired callback invoked when lock is acquired
    * @param onLockLost callback invoked when lock cannot be acquired or is lost
    */
   public EtcdDistributedLock(String etcdEndpoints,
                              String lockName,
                              int leaseTTLSeconds,
                              long keepAliveIntervalMs,
                              ScheduledExecutorService scheduledExecutor,
                              Runnable onLockAcquired,
                              Runnable onLockLost) {
      super(scheduledExecutor, scheduledExecutor, keepAliveIntervalMs, keepAliveIntervalMs, TimeUnit.MILLISECONDS, false);

      this.etcdClient = Client.builder()
         .endpoints(etcdEndpoints.split(","))
         .build();
      this.lockClient = etcdClient.getLockClient();
      this.leaseClient = etcdClient.getLeaseClient();
      this.lockKey = ByteSequence.from(lockName, StandardCharsets.UTF_8);
      this.leaseTTLSeconds = leaseTTLSeconds;
      this.onLockAcquired = onLockAcquired;
      this.onLockLost = onLockLost;
      this.lockHeld = false;
   }

   private boolean tryLock() {
      try {
         // Create a lease
         LeaseGrantResponse leaseResponse = leaseClient.grant(leaseTTLSeconds, 1, TimeUnit.SECONDS).get();
         currentLeaseId = leaseResponse.getID();

         // Try to acquire the lock with the lease
         LockResponse lockResponse = lockClient.lock(lockKey, currentLeaseId).get();
         currentLockKey = lockResponse.getKey();

         // Lock acquired successfully
         lockHeld = true;
         return true;

      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         // Failed to acquire lock
         cleanup();
         return false;
      }
   }

   @Override
   public void run() {
      // Synchronize the lock acquisition check to prevent races with stop()
      synchronized (this) {
         if (!lockHeld) {
            //logger.info("Trying to lock");
            // First time - try to acquire the lock
            if (tryLock()) {
               // Lock acquired successfully
               if (onLockAcquired != null) {
                  onLockAcquired.run();
               }
            }
            // Note: We do NOT call onLockLost when initial acquisition fails.
            // onLockLost should only be called when a previously held lock is lost.
            return;
         }
      }

      //logger.info("Keep Alive");
      // Lock already held - perform keep-alive check
      try {
         // Send keep-alive for the lease
         leaseClient.keepAliveOnce(currentLeaseId).get();
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         // Keep-alive failed - lock is lost
         handleLockLost();
      }
   }

   private synchronized void handleLockLost() {
      if (!lockHeld) {
         return;
      }

      lockHeld = false;

      if (onLockLost != null) {
         onLockLost.run();
      }
   }

   @Override
   public synchronized void stop() {
      super.stop();

      final boolean wasHeld = lockHeld;
      lockHeld = false;

      // we first call onLockLost
      // otherwise we could get into a race where the lock was already released and the
      // resource is unprotected
      if (wasHeld && onLockLost != null) {
         onLockLost.run();
      }

      try {
         // Unlock
         if (currentLockKey != null) {
            lockClient.unlock(currentLockKey).get();
         }
      } catch (Exception e) {
         // Ignore
      } finally {
         cleanup();
         try {
            etcdClient.close();
         } catch (Exception e) {
            // Ignore
         }
      }
   }

   private void cleanup() {
      try {
         if (currentLeaseId != 0) {
            leaseClient.revoke(currentLeaseId).get();
            currentLeaseId = 0;
         }
      } catch (Exception e) {
         // Ignore
      }
      currentLockKey = null;
   }

   public boolean isLocked() {
      return lockHeld;
   }
}
