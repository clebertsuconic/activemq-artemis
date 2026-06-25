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

/**
 * Base token for quota operations with automatic rollback.
 * <p>
 * Usage pattern:
 * <pre>
 * try (QuotaToken token = quotaService.acquireAddressToken(address, false)) {
 *    // ... perform operations ...
 *    token.commit(); // Success - don't rollback
 * } // Auto-rollback if commit() not called
 * </pre>
 */
public interface QuotaToken extends AutoCloseable {

   /**
    * Commit the quota change - prevents rollback on close.
    */
   void commit();

   /**
    * Release the token. Rolls back if not committed.
    */
   @Override
   void close();
}
