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

import org.apache.activemq.artemis.core.settings.impl.ResourceQuota;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;

/**
 * TransactionOperation that makes byte quota accounting participate in transaction lifecycle.
 *
 * <p>Quota is added optimistically at routing time (before transaction commits).
 * This operation ensures:
 * <ul>
 *   <li>On commit: quota stays added (afterCommit marks as committed)</li>
 *   <li>On rollback: quota is reversed (beforeRollback subtracts the bytes)</li>
 * </ul>
 *
 * <p>This pattern matches how PagingStore uses FinishPageMessageOperation to defer
 * page transaction commitment until the routing transaction commits.
 */
public class QuotaTransactionOperation implements TransactionOperation {

   private final ResourceQuota quota;
   private final int bytes;
   private final boolean sizeOnly;
   private boolean committed = false;

   /**
    * Create a quota transaction operation.
    *
    * @param quota the quota that already has bytes added optimistically
    * @param bytes the number of bytes to reverse on rollback
    * @param sizeOnly if true, only size is tracked (not element count)
    */
   public QuotaTransactionOperation(ResourceQuota quota, int bytes, boolean sizeOnly) {
      this.quota = quota;
      this.bytes = bytes;
      this.sizeOnly = sizeOnly;
      // Note: quota.addSize(bytes) has already been called by the time this is constructed
   }

   @Override
   public void beforePrepare(Transaction tx) throws Exception {
      // No preparation needed
   }

   @Override
   public void afterPrepare(Transaction tx) {
      // No action needed after prepare
   }

   @Override
   public void beforeCommit(Transaction tx) throws Exception {
      // No action needed before commit
   }

   @Override
   public void afterCommit(Transaction tx) {
      // Quota was already added in processRoute - just mark as committed
      // so beforeRollback won't reverse it
      committed = true;
   }

   @Override
   public void beforeRollback(Transaction tx) throws Exception {
      // Reverse the quota if transaction rolls back
      if (!committed && quota != null) {
         quota.addSize(-bytes, sizeOnly);
      }
   }

   @Override
   public void afterRollback(Transaction tx) {
      // No action needed after rollback
   }

   @Override
   public java.util.List<org.apache.activemq.artemis.core.server.MessageReference> getRelatedMessageReferences() {
      return java.util.Collections.emptyList();
   }

   @Override
   public java.util.List<org.apache.activemq.artemis.core.server.MessageReference> getListOnConsumer(long consumerID) {
      return java.util.Collections.emptyList();
   }
}
