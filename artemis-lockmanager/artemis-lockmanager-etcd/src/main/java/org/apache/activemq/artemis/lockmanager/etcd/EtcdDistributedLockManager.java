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
package org.apache.activemq.artemis.lockmanager.etcd;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lock.LockResponse;
import org.apache.activemq.artemis.lockmanager.DistributedLock;
import org.apache.activemq.artemis.lockmanager.DistributedLockManager;
import org.apache.activemq.artemis.lockmanager.MutableLong;
import org.apache.activemq.artemis.lockmanager.UnavailableStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdDistributedLockManager implements DistributedLockManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private Client etcdClient;
   private Lock lockClient;
   private Lease leaseClient;
   private KV kvClient;

   final EtcdLockConfiguration etcdLockConfiguration;


   public EtcdDistributedLockManager(EtcdLockConfiguration lockConfiguration) {
      this.etcdLockConfiguration = lockConfiguration;
   }

   public void connect() {
      ClientBuilder builder = Client.builder();
      builder.endpoints(etcdLockConfiguration.getEndpointArray());
      if (etcdLockConfiguration.getUser() != null) {
         builder.user(ByteSequence.from(etcdLockConfiguration.getUser(), StandardCharsets.UTF_8));
      }
      if (etcdLockConfiguration.getPassword() != null) {
         builder.password(ByteSequence.from(etcdLockConfiguration.getPassword(), StandardCharsets.UTF_8));
      }
      if (etcdLockConfiguration.getAuthority() != null) {
         builder.authority(etcdLockConfiguration.getAuthority());
      }

      etcdClient = builder.build();
      this.lockClient = etcdClient.getLockClient();
      this.leaseClient = etcdClient.getLeaseClient();
      this.kvClient = etcdClient.getKVClient();
   }


   class EtcdLock implements DistributedLock {

      final String lockId;
      private ByteSequence lockKey;

      private ByteSequence currentLockKey;
      private long currentLeaseId;

      EtcdLock(String lockId) {
         this.lockId = lockId;
         this.lockKey = ByteSequence.from(lockId, StandardCharsets.UTF_8);
      }

      @Override
      public String getLockId() {
         return lockId;
      }

      @Override
      public boolean isLockValid() throws UnavailableStateException {
         logger.debug("Checking lock validity with keep alive for lockId: {}", lockId);
         // Lock already held - perform keep-alive check
         try {
            // Send keep-alive for the lease
            LeaseKeepAliveResponse response = leaseClient.keepAliveOnce(currentLeaseId).get();
            if (response != null) {
               return true;
            } else {
               unlock();
               return false;
            }
         } catch (Exception e) {
            try {
               unlock();
            } catch (Exception unlockEx) {
               logger.debug("Error unlocking after keep-alive failure", unlockEx);
            }
            logger.warn(e.getMessage(), e);
            return false;
         }
      }

      @Override
      public boolean tryLock() throws UnavailableStateException, InterruptedException {
         return tryLock(0, TimeUnit.SECONDS);
      }

      @Override
      public boolean tryLock(long timeout, TimeUnit unit) throws UnavailableStateException, InterruptedException {

         logger.debug("Attempting to acquire lock for lockId: {} with lease period: {}s, timeout: {}{}",
                      lockId, etcdLockConfiguration.getLeasePeriod(), timeout, unit);
         try {
            // Create a lease
            LeaseGrantResponse leaseResponse;
            if (timeout <= 0) {
               leaseResponse = leaseClient.grant(etcdLockConfiguration.getLeasePeriod()).get();
            } else {
               leaseResponse = leaseClient.grant(etcdLockConfiguration.getLeasePeriod(), timeout, unit).get();
            }
            if (leaseResponse == null) {
               logger.debug("Lease grant returned null for lockId: {}", lockId);
               return false;
            }
            currentLeaseId = leaseResponse.getID();

            // Try to acquire the lock with the lease
            LockResponse lockResponse;
            if (timeout <= 0) {
               lockResponse = lockClient.lock(lockKey, currentLeaseId).get();
            } else {
               var lockFuture = lockClient.lock(lockKey, currentLeaseId);
               try {
                  lockResponse = lockFuture.get(timeout, unit);
               } catch (TimeoutException e) {
                  lockFuture.cancel(true);  // Cancel the lock acquisition attempt
                  lockResponse = null;
               }
            }
            if (lockResponse == null) {
               unlock();
               return false;
            }
            currentLockKey = lockResponse.getKey();

            // Lock acquired successfully
            return true;

         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            // Failed to acquire lock
            unlock();
            return false;
         }

      }

      @Override
      public void unlock() throws UnavailableStateException {
         try {
            try {
               if (currentLeaseId != 0) {
                  leaseClient.revoke(currentLeaseId).get();
                  currentLeaseId = 0;
               }
            } catch (Exception e) {
               // Ignore
            }
            // Unlock
            if (currentLockKey != null) {
               lockClient.unlock(currentLockKey).get();
            }
         } catch (Exception e) {
            // Ignore
         }
      }

      @Override
      public void addListener(UnavailableLockListener listener) {

      }

      @Override
      public void removeListener(UnavailableLockListener listener) {

      }

      @Override
      public void close() {

      }
   }

   class EtcdMutableLong implements MutableLong {

      private final String mutableLongId;
      private final ByteSequence key;

      EtcdMutableLong(String mutableLongId) {
         this.mutableLongId = mutableLongId;
         this.key = ByteSequence.from(mutableLongId, StandardCharsets.UTF_8);
      }

      @Override
      public String getMutableLongId() {
         return mutableLongId;
      }

      @Override
      public long get() throws UnavailableStateException {
         try {
            GetResponse response = kvClient.get(key).get();
            List<KeyValue> kvs = response.getKvs();

            if (kvs.isEmpty()) {
               // Key doesn't exist, return 0 as default
               return 0L;
            }

            ByteSequence valueBytes = kvs.get(0).getValue();
            byte[] bytes = valueBytes.getBytes();

            if (bytes.length != Long.BYTES) {
               throw new UnavailableStateException("Invalid long value stored for key: " + mutableLongId);
            }

            return ByteBuffer.wrap(bytes).getLong();
         } catch (Exception e) {
            throw new UnavailableStateException("Failed to get value for key: " + mutableLongId, e);
         }
      }

      @Override
      public void set(long value) throws UnavailableStateException {
         try {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(value);
            ByteSequence valueBytes = ByteSequence.from(buffer.array());

            kvClient.put(key, valueBytes).get();
         } catch (Exception e) {
            throw new UnavailableStateException("Failed to set value for key: " + mutableLongId, e);
         }
      }

      @Override
      public void close() {

      }
   }

   @Override
   public void addUnavailableManagerListener(UnavailableManagerListener listener) {

   }

   @Override
   public void removeUnavailableManagerListener(UnavailableManagerListener listener) {

   }

   @Override
   public boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      try {
         connect();
         return true;
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         return false;
      }
   }

   @Override
   public void start() throws InterruptedException, ExecutionException {
      try {
         connect();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public boolean isStarted() {
      return etcdClient != null;
   }

   @Override
   public DistributedLock getDistributedLock(String lockId) throws InterruptedException, ExecutionException, TimeoutException {
      return new EtcdLock(lockId);
   }

   @Override
   public MutableLong getMutableLong(String mutableLongId) throws InterruptedException, ExecutionException, TimeoutException {
      return new EtcdMutableLong(mutableLongId);
   }

   @Override
   public void stop() {
      if (leaseClient != null) {
         leaseClient.close();
         leaseClient = null;
      }
      if (lockClient != null) {
         lockClient.close();
         lockClient = null;
      }
      if (kvClient != null) {
         kvClient.close();
         kvClient = null;
      }

      if (etcdClient != null) {
         etcdClient.close();
         etcdClient = null;
      }
   }
}
