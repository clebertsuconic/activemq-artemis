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
import org.apache.activemq.artemis.core.server.quota.QueueQuotaToken;

/**
 * Generic no-op token used when no quota is configured.
 * Implements all quota token interfaces to eliminate duplication across NoOpAddressToken,
 * NoOpByteToken, and NoOpQueueToken.
 */
public enum NoOpToken implements AddressQuotaToken, QueueQuotaToken {
   INSTANCE;

   @Override
   public void commit() {
      // No-op: nothing to commit when quota is not enforced
   }

   @Override
   public void close() {
      // No-op: nothing to clean up when quota is not enforced
   }
}
