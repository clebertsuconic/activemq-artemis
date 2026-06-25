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

import org.apache.activemq.artemis.core.server.quota.AddressQuotaToken;
import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;

/**
 * Token for address removal.
 * Decrements quota only on commit, no rollback needed since quota wasn't modified yet.
 */
class AddressRemovalTokenImpl implements AddressQuotaToken {

   private final ResourceQuota quota;
   private volatile boolean committed = false;

   AddressRemovalTokenImpl(ResourceQuota quota) {
      this.quota = quota;
   }

   @Override
   public void commit() {
      committed = true;
      quota.decrementAddressCount();
   }

   @Override
   public void close() {
      // No rollback needed - if removal failed, we never decremented quota
   }
}
