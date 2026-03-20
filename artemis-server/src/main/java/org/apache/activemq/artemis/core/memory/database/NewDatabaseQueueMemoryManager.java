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

package org.apache.activemq.artemis.core.memory.database;

import org.apache.activemq.artemis.core.memory.AddressMemoryManager;
import org.apache.activemq.artemis.core.memory.QueueMemoryManager;
import org.apache.activemq.artemis.core.paging.cursor.PageIterator;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;

public class NewDatabaseQueueMemoryManager implements QueueMemoryManager {

   @Override
   public boolean supportsDirectDelivery() {
      return false;
   }

   @Override
   public AddressMemoryManager getAddressMemoryManager() {
      return null;
   }

   @Override
   public void setQueue(Queue queue) {

   }

   @Override
   public void pageAckTx(Transaction tx, PagedReference ref) throws Exception {

   }

   @Override
   public PageIterator pageIterator(boolean browsing) {
      return null;
   }

   @Override
   public void notEmpty() {

   }

   @Override
   public boolean isPaging() {
      return false;
   }

   @Override
   public boolean contains(MessageReference reference) throws Exception {
      return false;
   }

   @Override
   public void destroy() throws Exception {

   }

   @Override
   public boolean isStorePaging() {
      return false;
   }

   @Override
   public long getMessageCount() {
      return 0;
   }

   @Override
   public long getPersistentSize() {
      return 0;
   }

   @Override
   public void pageAck(PagedReference ref) throws Exception {

   }

   @Override
   public PageSubscriptionCounter getPageCounter() {
      return null;
   }
}
