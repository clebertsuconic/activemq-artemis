/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.impl.jdbc;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.artemis.jdbc.store.drivers.AbstractJDBCDriver;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.lockmanager.DistributedLock;
import org.apache.activemq.artemis.lockmanager.DistributedLockManager;
import org.apache.activemq.artemis.lockmanager.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * JDBC-based implementation of {@link DistributedLockManager}.
 * <p>
 * This implementation uses JDBC lease locks to provide distributed locking capabilities.
 * <p>
 * Valid configuration parameters:
 * <ul>
 *   <li><b>holder-id</b> (required): Unique identifier for this lock manager instance</li>
 *   <li><b>lock-expiration-millis</b> (optional): Lock expiration time in milliseconds (default: 30000)</li>
 *   <li><b>query-timeout-millis</b> (optional): JDBC query timeout in milliseconds (default: -1, disabled)</li>
 *   <li><b>allowed-time-diff</b> (optional): Allowed time difference between local and database time in milliseconds (default: 5000)</li>
 * </ul>
 */
public class JdbcDistributedLockManager implements DistributedLockManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final long DEFAULT_LOCK_EXPIRATION_MILLIS = 30000;
   private static final long DEFAULT_QUERY_TIMEOUT_MILLIS = -1;
   private static final long DEFAULT_ALLOWED_TIME_DIFF = 5000;

   private final String holderId;
   private JDBCConnectionProvider connectionProvider;
   private SQLProvider sqlProvider;
   private final long lockExpirationMillis;
   private final long queryTimeoutMillis;
   private final long allowedTimeDiff;
   private final Map<String, JdbcDistributedLock> locks;
   private final CopyOnWriteArrayList<UnavailableManagerListener> listeners;
   private volatile boolean started;

   public JdbcDistributedLockManager(Map<String, String> config) {
      this(
         Objects.requireNonNull(config.get("holder-id"), "holder-id is required"),
         null, // connectionProvider must be set separately
         null, // sqlProvider must be set separately
         parseLong(config, "lock-expiration-millis", DEFAULT_LOCK_EXPIRATION_MILLIS),
         parseLong(config, "query-timeout-millis", DEFAULT_QUERY_TIMEOUT_MILLIS),
         parseLong(config, "allowed-time-diff", DEFAULT_ALLOWED_TIME_DIFF)
      );
   }

   public JdbcDistributedLockManager(String holderId,
                                     JDBCConnectionProvider connectionProvider,
                                     SQLProvider sqlProvider,
                                     long lockExpirationMillis,
                                     long queryTimeoutMillis,
                                     long allowedTimeDiff) {
      this.holderId = Objects.requireNonNull(holderId, "holderId cannot be null");
      this.connectionProvider = connectionProvider;
      this.sqlProvider = sqlProvider;
      this.lockExpirationMillis = lockExpirationMillis;
      this.queryTimeoutMillis = queryTimeoutMillis;
      this.allowedTimeDiff = allowedTimeDiff;
      this.locks = new ConcurrentHashMap<>();
      this.listeners = new CopyOnWriteArrayList<>();
      this.started = false;
   }

   private static long parseLong(Map<String, String> config, String key, long defaultValue) {
      String value = config.get(key);
      return value != null ? Long.parseLong(value) : defaultValue;
   }

   public void setConnectionProvider(JDBCConnectionProvider connectionProvider) {
      if (started) {
         throw new IllegalStateException("Cannot set connection provider after manager has started");
      }
      if (this.connectionProvider != null && connectionProvider != this.connectionProvider) {
         throw new IllegalStateException("Connection provider already set");
      }
      this.connectionProvider = connectionProvider;
   }

   public void setSqlProvider(SQLProvider sqlProvider) {
      if (started) {
         throw new IllegalStateException("Cannot set SQL provider after manager has started");
      }
      if (this.sqlProvider != null && sqlProvider != this.sqlProvider) {
         throw new IllegalStateException("SQL provider already set");
      }
      this.sqlProvider = sqlProvider;
   }

   @Override
   public void addUnavailableManagerListener(UnavailableManagerListener listener) {
      if (listener != null) {
         listeners.add(listener);
      }
   }

   @Override
   public void removeUnavailableManagerListener(UnavailableManagerListener listener) {
      if (listener != null) {
         listeners.remove(listener);
      }
   }

   @Override
   public boolean start(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
      if (timeout >= 0) {
         Objects.requireNonNull(unit);
      }
      if (started) {
         return true;
      }
      if (connectionProvider == null) {
         throw new IllegalStateException("Connection provider must be set before starting");
      }
      if (sqlProvider == null) {
         throw new IllegalStateException("SQL provider must be set before starting");
      }
      started = true;
      logger.info("JDBC Distributed Lock Manager started with holder ID: {}", holderId);
      return true;
   }

   @Override
   public void start() throws InterruptedException, ExecutionException {
      start(-1, null);
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public void stop() {
      if (!started) {
         return;
      }
      try {
         locks.forEach((lockId, lock) -> {
            try {
               lock.close();
            } catch (Throwable t) {
               logger.warn("Error closing lock {}", lockId, t);
            }
         });
         locks.clear();
      } finally {
         started = false;
         logger.info("JDBC Distributed Lock Manager stopped");
      }
   }

   @Override
   public DistributedLock getDistributedLock(String lockId) throws InterruptedException, ExecutionException, TimeoutException {
      Objects.requireNonNull(lockId, "lockId cannot be null");
      if (!started) {
         throw new IllegalStateException("Manager must be started first");
      }

      return locks.computeIfAbsent(lockId, id -> {
         try {
            return createLeaseLock(id);
         } catch (Exception e) {
            logger.error("Error creating distributed lock for {}", id, e);
            notifyUnavailableListeners();
            throw new RuntimeException("Failed to create distributed lock", e);
         }
      });
   }

   private JdbcDistributedLock createLeaseLock(String lockId) {
      String uniqueHolderId = holderId + "-" + lockId;
      return new JdbcDistributedLock(
         uniqueHolderId,
         connectionProvider,
         sqlProvider.tryAcquirePrimaryLockSQL(),
         sqlProvider.tryReleasePrimaryLockSQL(),
         sqlProvider.renewPrimaryLockSQL(),
         sqlProvider.isPrimaryLockedSQL(),
         sqlProvider.currentTimestampSQL(),
         sqlProvider.currentTimestampTimeZoneId(),
         lockExpirationMillis,
         queryTimeoutMillis,
         lockId,
         allowedTimeDiff
      );
   }

   @Override
   public MutableLong getMutableLong(String mutableLongId) throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException("MutableLong is not supported by JDBC lock manager");
   }

   private void notifyUnavailableListeners() {
      for (UnavailableManagerListener listener : listeners) {
         try {
            listener.onUnavailableManagerEvent();
         } catch (Throwable t) {
            logger.warn("Error notifying unavailable manager listener", t);
         }
      }
   }

   public static class LockManagerDriver extends AbstractJDBCDriver {

      public LockManagerDriver(JDBCConnectionProvider connectionProvider,
                               SQLProvider provider) {
         super(connectionProvider, provider);
      }

      @Override
      protected void prepareStatements() {

      }
      @Override
      protected void createSchema() {
         try {
            createTable(sqlProvider.createNodeManagerStoreTableSQL(), sqlProvider.createNodeIdSQL(), sqlProvider.createStateSQL(), sqlProvider.createPrimaryLockSQL(), sqlProvider.createBackupLockSQL());
         } catch (SQLException e) {
            //no op: if a table already exists is not a problem in this case, the prepareStatements() call will fail right after it if the table is not correctly initialized
            logger.debug("Error while creating the schema of the JDBC shared state manager", e);
         }
      }
   }
}
