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
package org.apache.activemq.artemis.core.server.quota.impl;

import org.apache.activemq.artemis.api.core.ActiveMQResourceQuotaExceededException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.ResourceQuotaManager;
import org.apache.activemq.artemis.core.server.quota.AddressQuotaToken;
import org.apache.activemq.artemis.core.server.quota.QueueQuotaToken;
import org.apache.activemq.artemis.core.server.quota.ResourceQuotaService;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Implementation of ResourceQuotaService following ActiveMQComponent lifecycle.
 */
public class ResourceQuotaServiceImpl implements ResourceQuotaService {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;
   private final org.apache.activemq.artemis.core.config.Configuration configuration;
   private volatile boolean started;
   private ResourceQuotaManager resourceQuotaManager;

   public ResourceQuotaServiceImpl(HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                   org.apache.activemq.artemis.core.config.Configuration configuration) {
      this.addressSettingsRepository = addressSettingsRepository;
      this.configuration = configuration;
      this.started = false;
   }

   @Override
   public void start() throws Exception {
      logger.debug("Starting ResourceQuotaService");

      // Create and initialize ResourceQuotaManager
      createAndInitializeQuotaManager();

      started = true;
   }

   /**
    * Create ResourceQuotaManager and initialize it with runtime quota instances from configuration.
    * Creates ResourceQuota instances from ResourceQuotaConfig definitions.
    * Counters start at zero and are rebuilt by scanning existing addresses/queues during broker startup.
    */
   private void createAndInitializeQuotaManager() {
      if (configuration == null) {
         return;
      }

      java.util.Map<String, org.apache.activemq.artemis.core.settings.impl.ResourceQuotaConfig> quotaConfigs =
         configuration.getResourceQuotaConfigs();
      if (quotaConfigs == null || quotaConfigs.isEmpty()) {
         logger.debug("No quota configurations found, skipping ResourceQuotaManager creation");
         return;
      }

      // Create ResourceQuotaManager
      org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository<ResourceQuota> quotaRepo =
         new org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository<>(
            configuration.getWildcardConfiguration());

      resourceQuotaManager = new ResourceQuotaManager(quotaRepo, configuration.getWildcardConfiguration());

      java.util.Map<String, ResourceQuota> runtimeQuotas = new java.util.HashMap<>();

      // Create runtime ResourceQuota instances from configuration
      for (java.util.Map.Entry<String, org.apache.activemq.artemis.core.settings.impl.ResourceQuotaConfig> entry : quotaConfigs.entrySet()) {
         ResourceQuota runtimeQuota = entry.getValue().createRuntimeQuota();
         runtimeQuotas.put(entry.getKey(), runtimeQuota);
         resourceQuotaManager.addQuota(entry.getKey(), runtimeQuota);
      }

      // Establish parent relationships between runtime quota instances
      resourceQuotaManager.establishParentRelationships(runtimeQuotas);

      logger.info("Created ResourceQuotaManager with {} runtime quota instances", runtimeQuotas.size());
   }

   /**
    * Get the ResourceQuotaManager instance.
    * {@return the ResourceQuotaManager, or null if not initialized}
    */
   @Override
   public ResourceQuotaManager getResourceQuotaManager() {
      return resourceQuotaManager;
   }

   @Override
   public void stop() throws Exception {
      logger.debug("Stopping ResourceQuotaService");
      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public AddressQuotaToken acquireAddressToken(SimpleString address, boolean reload) throws Exception {
      ResourceQuota quota = lookupQuota(address);
      if (quota == null) {
         return NoOpToken.INSTANCE;
      }

      // During reload, skip enforcement but still rebuild counts
      if (reload) {
         // Increment counter to rebuild quota state from persisted addresses
         // Don't check limits - we're restoring previously valid state
         quota.incrementAddressCount();
         // Return auto-commit token (no rollback needed during reload)
         return ReloadAddressToken.INSTANCE;
      }

      // Normal operation: enforce quota before incrementing
      if (!quota.tryIncrementAddressCount()) {
         throw new ActiveMQResourceQuotaExceededException(
            "Address quota exceeded for quota '" + quota.getName() +
            "': max addresses is " + quota.getMaxAddresses());
      }

      return new AddressQuotaTokenImpl(quota);
   }

   @Override
   public QueueQuotaToken acquireQueueToken(SimpleString address, boolean reload) throws Exception {
      ResourceQuota quota = lookupQuota(address);
      if (quota == null) {
         return NoOpToken.INSTANCE;
      }

      // During reload, skip enforcement but still rebuild counts
      if (reload) {
         // Increment counter to rebuild quota state from persisted queues
         // Don't check limits - we're restoring previously valid state
         quota.incrementQueueCount();
         // Return auto-commit token (no rollback needed during reload)
         return ReloadQueueToken.INSTANCE;
      }

      // Normal operation: enforce quota before incrementing
      if (!quota.tryIncrementQueueCount()) {
         throw new ActiveMQResourceQuotaExceededException(
            "Queue quota exceeded for quota '" + quota.getName() +
            "': max queues is " + quota.getMaxQueues());
      }

      return new QueueQuotaTokenImpl(quota);
   }

   @Override
   public ResourceQuota lookupQuota(SimpleString address) {
      if (resourceQuotaManager == null) {
         return null;
      }

      AddressSettings settings = addressSettingsRepository.getMatch(address.toString());
      return resourceQuotaManager.getQuotaForAddress(address, settings);
   }

   @Override
   public AddressQuotaToken acquireAddressRemovalToken(SimpleString address) {
      ResourceQuota quota = lookupQuota(address);
      if (quota == null) {
         return NoOpToken.INSTANCE;
      }
      return new AddressRemovalTokenImpl(quota);
   }

   @Override
   public QueueQuotaToken acquireQueueRemovalToken(SimpleString address) {
      ResourceQuota quota = lookupQuota(address);
      if (quota == null) {
         return NoOpToken.INSTANCE;
      }
      return new QueueRemovalTokenImpl(quota);
   }

   /**
    * Reload token for addresses - auto-commits, no rollback needed.
    * Used during journal replay to rebuild quota counts from persisted state.
    */
   private enum ReloadAddressToken implements AddressQuotaToken {
      INSTANCE;

      @Override
      public void commit() {
         // Already committed during reload - this is a no-op
      }

      @Override
      public void close() {
         // No rollback needed during reload
      }
   }

   /**
    * Reload token for queues - auto-commits, no rollback needed.
    * Used during journal replay to rebuild quota counts from persisted state.
    */
   private enum ReloadQueueToken implements QueueQuotaToken {
      INSTANCE;

      @Override
      public void commit() {
         // Already committed during reload - this is a no-op
      }

      @Override
      public void close() {
         // No rollback needed during reload
      }
   }
}
