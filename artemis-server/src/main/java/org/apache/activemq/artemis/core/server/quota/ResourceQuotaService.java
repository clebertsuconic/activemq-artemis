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
package org.apache.activemq.artemis.core.server.quota;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;

/**
 * Service for managing resource quota enforcement with automatic rollback semantics.
 * <p>
 * This service provides try-with-resources tokens that automatically rollback quota
 * changes on failure and require explicit commit() for success. This eliminates
 * error-prone manual increment/decrement code.
 */
public interface ResourceQuotaService extends ActiveMQComponent {

   /**
    * Acquire address quota token. Auto-rollback on close, commit on success.
    *
    * @param address the address name
    * @param reload if true, skip quota checks (for reload mode)
    * @return token (may be no-op if no quota configured)
    * @throws Exception if quota exceeded
    */
   AddressQuotaToken acquireAddressToken(SimpleString address, boolean reload) throws Exception;

   /**
    * Acquire queue quota token. Auto-rollback on close, commit on success.
    *
    * @param address the address name for quota lookup
    * @param reload if true, skip quota checks (for reload mode)
    * @return token (may be no-op if no quota configured)
    * @throws Exception if quota exceeded
    */
   QueueQuotaToken acquireQueueToken(SimpleString address, boolean reload) throws Exception;

   /**
    * Acquire address removal token. Decrements on commit, increments back on rollback.
    *
    * @param address the address name
    * @return token (may be no-op if no quota configured)
    */
   AddressQuotaToken acquireAddressRemovalToken(SimpleString address);

   /**
    * Acquire queue removal token. Decrements on commit, increments back on rollback.
    *
    * @param address the address name for quota lookup
    * @return token (may be no-op if no quota configured)
    */
   QueueQuotaToken acquireQueueRemovalToken(SimpleString address);

   /**
    * Lookup the resource quota for a given address.
    * This looks up which quota applies to the address via AddressSettings.
    *
    * @param address the address name
    * @return the ResourceQuota instance, or null if no quota is configured
    */
   org.apache.activemq.artemis.core.settings.impl.ResourceQuota lookupQuota(SimpleString address);

   /**
    * Get a resource quota by its configured name.
    * This provides direct access to a quota by the name it was configured with.
    *
    * @param quotaName the quota name
    * @return the ResourceQuota instance, or null if no quota with that name exists
    */
   default org.apache.activemq.artemis.core.settings.impl.ResourceQuota getQuotaByName(String quotaName) {
      org.apache.activemq.artemis.core.paging.ResourceQuotaManager manager = getResourceQuotaManager();
      return manager != null ? manager.getQuota(quotaName) : null;
   }

   /**
    * Get the ResourceQuotaManager managed by this service.
    * This provides access to the manager for operations that need direct access
    * to the hierarchy and wildcard template functionality.
    *
    * @return the ResourceQuotaManager instance, or null if quotas are not configured
    */
   default org.apache.activemq.artemis.core.paging.ResourceQuotaManager getResourceQuotaManager() {
      return null;
   }
}
