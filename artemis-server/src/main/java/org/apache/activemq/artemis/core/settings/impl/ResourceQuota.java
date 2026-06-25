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
package org.apache.activemq.artemis.core.settings.impl;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.utils.SizeAwareMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Runtime resource quota tracker for hierarchical resource management.
 * <p>
 * This class tracks live quota usage (counters) and enforces limits defined in {@link ResourceQuotaConfig}.
 * ResourceQuota instances are NOT serializable - they are always rebuilt on broker restart by:
 * <ol>
 *   <li>Creating from ResourceQuotaConfig via {@link ResourceQuotaConfig#createRuntimeQuota()}</li>
 *   <li>Scanning existing addresses/queues to rebuild counters (during journal replay)</li>
 * </ol>
 * <p>
 * Three types of limits are enforced:
 * <ul>
 *   <li>max-message-bytes: Total bytes for messages across all addresses in this quota</li>
 *   <li>max-addresses: Maximum number of addresses in this quota</li>
 *   <li>max-queues: Maximum number of queues in this quota</li>
 * </ul>
 * <p>
 * Quotas can be organized in a parent-child hierarchy where child quotas count toward parent limits.
 * Quotas support wildcard templates via {@link ResourceQuotaConfig} that create runtime instances
 * on-demand when addresses match patterns.
 *
 * @see ResourceQuotaConfig for the configuration/limits definition
 */
public class ResourceQuota {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final long DEFAULT_MAX_MESSAGE_BYTES = -1;
   public static final int DEFAULT_MAX_ADDRESSES = -1;
   public static final int DEFAULT_MAX_QUEUES = -1;

   // Configuration (limits) - set from ResourceQuotaConfig
   private String name;
   private String partOf;
   private Long maxMessageBytes;
   private Integer maxAddresses;
   private Integer maxQueues;

   // Runtime state (counters, metrics, relationships)
   private ResourceQuota parent;
   private SizeAwareMetric sizeMetric;
   private AtomicInteger addressCount;
   private AtomicInteger queueCount;
   private Runnable overCallback;
   private Runnable underCallback;

   // ========================================================================
   // Constructor and Initialization
   // ========================================================================

   /**
    * Create a runtime quota instance. Typically called via {@link ResourceQuotaConfig#createRuntimeQuota()}.
    * Counters start at zero and are rebuilt by scanning existing addresses/queues.
    *
    * @param name the quota name
    */
   public ResourceQuota(String name) {
      this.name = name;
      this.partOf = null;
      this.maxMessageBytes = null;
      this.maxAddresses = null;
      this.maxQueues = null;
      initializeRuntimeState();
   }

   /**
    * Initialize transient runtime state (counters, metrics, callbacks).
    * Called on construction and lazily after deserialization via ensureInitialized().
    * This allows ResourceQuota instances to be serialized as configuration and
    * automatically initialize runtime state when used.
    */
   private void initializeRuntimeState() {
      this.addressCount = new AtomicInteger(0);
      this.queueCount = new AtomicInteger(0);

      // SizeAwareMetric will be initialized when limits are set
      // This allows proper lower-mark calculation
      long maxBytes = getMaxMessageBytes();
      long lowerMarkBytes = maxBytes > 0 ? (long) (maxBytes * 0.9) : -1;

      this.sizeMetric = new SizeAwareMetric(maxBytes, lowerMarkBytes, -1, -1);

      // Set up callback to propagate to parent
      this.sizeMetric.setOnSizeCallback((delta, sizeOnly) -> {
         if (parent != null) {
            parent.addSize(delta, sizeOnly);
         }
      });
   }

   // ========================================================================
   // Configuration Getters and Setters
   // ========================================================================

   public String getName() {
      return name;
   }

   public String getPartOf() {
      return partOf;
   }

   public ResourceQuota setPartOf(String partOf) {
      this.partOf = partOf;
      return this;
   }

   public long getMaxMessageBytes() {
      return maxMessageBytes != null ? maxMessageBytes : DEFAULT_MAX_MESSAGE_BYTES;
   }

   public ResourceQuota setMaxMessageBytes(Long maxMessageBytes) {
      this.maxMessageBytes = maxMessageBytes;
      // Reinitialize metric when limits change
      if (sizeMetric != null) {
         long maxBytes = getMaxMessageBytes();
         long lowerMarkBytes = maxBytes > 0 ? (long) (maxBytes * 0.9) : -1;
         sizeMetric.setMax(maxBytes, lowerMarkBytes, -1, -1);
      }
      return this;
   }

   public int getMaxAddresses() {
      return maxAddresses != null ? maxAddresses : DEFAULT_MAX_ADDRESSES;
   }

   public ResourceQuota setMaxAddresses(Integer maxAddresses) {
      this.maxAddresses = maxAddresses;
      return this;
   }

   public int getMaxQueues() {
      return maxQueues != null ? maxQueues : DEFAULT_MAX_QUEUES;
   }

   public ResourceQuota setMaxQueues(Integer maxQueues) {
      this.maxQueues = maxQueues;
      return this;
   }

   public ResourceQuota getParent() {
      return parent;
   }

   public void setParent(ResourceQuota parent) {
      this.parent = parent;
   }

   // ========================================================================
   // Byte Quota Operations
   // ========================================================================

   /**
    * Add size delta to this quota and propagate to parent via callback chain.
    *
    * @param delta      size change in bytes (can be negative for decrements)
    * @param sizeOnly   if true, don't increment element count
    * @return new total size after adding delta
    */
   public long addSize(int delta, boolean sizeOnly) {
      ensureInitialized();
      return sizeMetric.addSize(delta, sizeOnly);
   }

   /**
    * Get current size in bytes tracked by this quota.
    * Alias for getCurrentMessageBytes() for consistency with other getCurrentXXX methods.
    */
   public long getSize() {
      return getCurrentMessageBytes();
   }

   /**
    * Get current message bytes tracked by this quota.
    */
   public long getCurrentMessageBytes() {
      ensureInitialized();
      return sizeMetric.getSize();
   }

   /**
    * Get current element count tracked by this quota.
    */
   public long getElements() {
      ensureInitialized();
      return sizeMetric.getElements();
   }

   // ========================================================================
   // Limit Checking Methods
   // ========================================================================

   /**
    * Check if any limit is exceeded (bytes, addresses, or queues).
    *
    * @return true if any limit is exceeded
    */
   public boolean isOverLimit() {
      ensureInitialized();
      return isOverByteLimit() || isOverAddressLimit() || isOverQueueLimit();
   }

   /**
    * Check if byte limit is exceeded.
    *
    * @return true if current message bytes exceed maxMessageBytes
    */
   public boolean isOverByteLimit() {
      ensureInitialized();
      return sizeMetric.isOver();
   }

   /**
    * Check if address limit is exceeded.
    *
    * @return true if current address count exceeds maxAddresses
    */
   public boolean isOverAddressLimit() {
      if (maxAddresses == null || maxAddresses < 0) {
         return false;
      }
      return addressCount.get() > maxAddresses;
   }

   /**
    * Check if queue limit is exceeded.
    *
    * @return true if current queue count exceeds maxQueues
    */
   public boolean isOverQueueLimit() {
      if (maxQueues == null || maxQueues < 0) {
         return false;
      }
      return queueCount.get() > maxQueues;
   }

   /**
    * Check if this quota has any limits configured.
    *
    * @return true if at least one limit (bytes, addresses, or queues) is configured
    */
   public boolean hasLimits() {
      return (maxMessageBytes != null && maxMessageBytes >= 0) ||
             (maxAddresses != null && maxAddresses >= 0) ||
             (maxQueues != null && maxQueues >= 0);
   }

   /**
    * Get percentage of byte limit used.
    *
    * @return percentage (0-100) or -1 if no limit configured
    */
   public double getByteUtilizationPercent() {
      if (maxMessageBytes == null || maxMessageBytes <= 0) {
         return -1;
      }
      ensureInitialized();
      return (sizeMetric.getSize() * 100.0) / maxMessageBytes;
   }

   /**
    * Get percentage of address limit used.
    *
    * @return percentage (0-100) or -1 if no limit configured
    */
   public double getAddressUtilizationPercent() {
      if (maxAddresses == null || maxAddresses <= 0) {
         return -1;
      }
      ensureInitialized();
      return (addressCount.get() * 100.0) / maxAddresses;
   }

   /**
    * Get percentage of queue limit used.
    *
    * @return percentage (0-100) or -1 if no limit configured
    */
   public double getQueueUtilizationPercent() {
      if (maxQueues == null || maxQueues <= 0) {
         return -1;
      }
      ensureInitialized();
      return (queueCount.get() * 100.0) / maxQueues;
   }

   // ========================================================================
   // Address Counter Operations
   // ========================================================================

   /**
    * Atomically increment address count if within limit.
    * Checks limit and increments in a single atomic operation to prevent race conditions.
    *
    * @return true if increment succeeded, false if would exceed limit
    */
   public boolean tryIncrementAddressCount() {
      ensureInitialized();

      boolean parentIncremented = false;
      try {
         // If parent has quota, check parent first (parent quota is more restrictive)
         if (parent != null) {
            if (!parent.tryIncrementAddressCount()) {
               return false;
            }
            parentIncremented = true;
         }

         // Atomically check and increment using compareAndSet loop
         while (true) {
            int current = addressCount.get();

            // Check if incrementing would exceed limit
            if (maxAddresses != null && maxAddresses >= 0 && current >= maxAddresses) {
               logger.debug("Quota {} address limit {} reached at count {}", name, maxAddresses, current);
               return false;
            }

            // Try to increment atomically
            if (addressCount.compareAndSet(current, current + 1)) {
               logger.debug("Quota {} address count incremented to {}", name, current + 1);
               parentIncremented = false; // Success, don't rollback in finally
               return true;
            }
            // If CAS failed, another thread modified the value - loop and retry
         }
      } finally {
         // Rollback parent if we incremented it but failed to increment self
         if (parentIncremented && parent != null) {
            parent.decrementAddressCount();
         }
      }
   }

   /**
    * Increment address count and propagate to parent.
    * Note: This is not atomic with limit checking - use tryIncrementAddressCount() to avoid race conditions.
    */
   public void incrementAddressCount() {
      ensureInitialized();
      addressCount.incrementAndGet();
      if (parent != null) {
         parent.incrementAddressCount();
      }
      logger.debug("Quota {} address count incremented to {}", name, addressCount.get());
   }

   /**
    * Decrement address count and propagate to parent
    */
   public void decrementAddressCount() {
      ensureInitialized();
      int current = addressCount.decrementAndGet();
      if (current < 0) {
         logger.warn("Quota {} address count went negative: {} (possible double-decrement bug)", name, current);
         addressCount.set(0);
         // Don't propagate to parent when count went negative - indicates a bug
         return;
      }
      if (parent != null) {
         parent.decrementAddressCount();
      }
      logger.debug("Quota {} address count decremented to {}", name, addressCount.get());
   }

   // ========================================================================
   // Queue Counter Operations
   // ========================================================================

   /**
    * Atomically increment queue count if within limit.
    * Checks limit and increments in a single atomic operation to prevent race conditions.
    *
    * @return true if increment succeeded, false if would exceed limit
    */
   public boolean tryIncrementQueueCount() {
      ensureInitialized();

      boolean parentIncremented = false;
      try {
         // If parent has quota, check parent first (parent quota is more restrictive)
         if (parent != null) {
            if (!parent.tryIncrementQueueCount()) {
               return false;
            }
            parentIncremented = true;
         }

         // Atomically check and increment using compareAndSet loop
         while (true) {
            int current = queueCount.get();

            // Check if incrementing would exceed limit
            if (maxQueues != null && maxQueues >= 0 && current >= maxQueues) {
               logger.debug("Quota {} queue limit {} reached at count {}", name, maxQueues, current);
               return false;
            }

            // Try to increment atomically
            if (queueCount.compareAndSet(current, current + 1)) {
               logger.debug("Quota {} queue count incremented to {}", name, current + 1);
               parentIncremented = false; // Success, don't rollback in finally
               return true;
            }
            // If CAS failed, another thread modified the value - loop and retry
         }
      } finally {
         // Rollback parent if we incremented it but failed to increment self
         if (parentIncremented && parent != null) {
            parent.decrementQueueCount();
         }
      }
   }

   /**
    * Increment queue count and propagate to parent.
    * Note: This is not atomic with limit checking - use tryIncrementQueueCount() to avoid race conditions.
    */
   public void incrementQueueCount() {
      ensureInitialized();
      queueCount.incrementAndGet();
      if (parent != null) {
         parent.incrementQueueCount();
      }
      logger.debug("Quota {} queue count incremented to {}", name, queueCount.get());
   }

   /**
    * Decrement queue count and propagate to parent
    */
   public void decrementQueueCount() {
      ensureInitialized();
      int current = queueCount.decrementAndGet();
      if (current < 0) {
         logger.warn("Quota {} queue count went negative: {} (possible double-decrement bug)", name, current);
         queueCount.set(0);
         // Don't propagate to parent when count went negative - indicates a bug
         return;
      }
      if (parent != null) {
         parent.decrementQueueCount();
      }
      logger.debug("Quota {} queue count decremented to {}", name, queueCount.get());
   }

   /**
    * Get current address count tracked by this quota.
    */
   public int getCurrentAddressCount() {
      ensureInitialized();
      return addressCount.get();
   }

   /**
    * Alias for getCurrentAddressCount() for backward compatibility.
    */
   public int getAddressCount() {
      return getCurrentAddressCount();
   }

   /**
    * Get current queue count tracked by this quota.
    */
   public int getCurrentQueueCount() {
      ensureInitialized();
      return queueCount.get();
   }

   /**
    * Alias for getCurrentQueueCount() for backward compatibility.
    */
   public int getQueueCount() {
      return getCurrentQueueCount();
   }

   // ========================================================================
   // Callback Configuration
   // ========================================================================

   /**
    * Set callback to be invoked when quota goes over limit.
    */
   public void setOverCallback(Runnable callback) {
      ensureInitialized();
      this.overCallback = callback;
      this.sizeMetric.setOverCallback(callback);
   }

   /**
    * Set callback to be invoked when quota goes under limit.
    */
   public void setUnderCallback(Runnable callback) {
      ensureInitialized();
      this.underCallback = callback;
      this.sizeMetric.setUnderCallback(callback);
   }

   // ========================================================================
   // Lifecycle and Internal Methods
   // ========================================================================

   /**
    * Ensure runtime state is initialized (handles deserialization).
    */
   private void ensureInitialized() {
      if (sizeMetric == null) {
         initializeRuntimeState();
      }
   }

   // ========================================================================
   // Copy Method (for wildcard template instantiation)
   // ========================================================================

   /**
    * Create a copy of this runtime quota for wildcard template instantiation.
    * The copy has the same limits but fresh counters (starting at zero).
    * This is used when a wildcard template (e.g., "region.*") creates a specific instance (e.g., "region.us").
    * Counters are NOT copied - new instance starts at zero and will be rebuilt by scanning.
    *
    * @param newName the name for the new quota instance
    * @return new ResourceQuota with same limits but zero counters
    */
   public ResourceQuota copy(String newName) {
      ResourceQuota copy = new ResourceQuota(newName);
      copy.maxMessageBytes = this.maxMessageBytes;
      copy.maxAddresses = this.maxAddresses;
      copy.maxQueues = this.maxQueues;
      copy.partOf = this.partOf;
      // Counters start at zero - will be rebuilt
      return copy;
   }

   // ========================================================================
   // Object Methods (equals, hashCode, toString)
   // ========================================================================

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof ResourceQuota)) {
         return false;
      }
      ResourceQuota that = (ResourceQuota) o;
      return Objects.equals(name, that.name) &&
             Objects.equals(partOf, that.partOf) &&
             Objects.equals(maxMessageBytes, that.maxMessageBytes) &&
             Objects.equals(maxAddresses, that.maxAddresses) &&
             Objects.equals(maxQueues, that.maxQueues);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, partOf, maxMessageBytes, maxAddresses, maxQueues);
   }

   @Override
   public String toString() {
      return "ResourceQuota{" +
             "name='" + name + '\'' +
             ", partOf='" + partOf + '\'' +
             ", maxMessageBytes=" + maxMessageBytes +
             ", maxAddresses=" + maxAddresses +
             ", maxQueues=" + maxQueues +
             ", currentSize=" + (sizeMetric != null ? sizeMetric.getSize() : 0) +
             ", currentAddresses=" + (addressCount != null ? addressCount.get() : 0) +
             ", currentQueues=" + (queueCount != null ? queueCount.get() : 0) +
             '}';
   }
}
