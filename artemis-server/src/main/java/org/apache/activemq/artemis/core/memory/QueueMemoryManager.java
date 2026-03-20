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

package org.apache.activemq.artemis.core.memory;

import org.apache.activemq.artemis.core.paging.cursor.PageIterator;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;

public interface QueueMemoryManager {

   boolean supportsDirectDelivery();

   AddressMemoryManager getAddressMemoryManager();

   void setQueue(Queue queue);

   boolean isPaging();

   boolean contains(MessageReference reference) throws Exception;

   void destroy() throws Exception;

   PageIterator pageIterator(boolean browsing);

   boolean isStorePaging();

   long getMessageCount();

   long getPersistentSize();

   /**
    * This will be ignored on Non Paged Managers
    */
   void pageAckTx(Transaction tx, PagedReference ref) throws Exception;

   /**
    * This will be ignored on Non Paged Managers
    */
   void pageAck(PagedReference ref) throws Exception;

   PageSubscriptionCounter getPageCounter();

   void notEmpty();
}
