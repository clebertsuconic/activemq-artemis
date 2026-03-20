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
package org.apache.activemq.artemis.core.paging.cursor;

import java.util.function.Consumer;

import org.apache.activemq.artemis.core.memory.QueueMemoryManager;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;

public interface PageSubscription extends QueueMemoryManager {

   // Cursor query operations --------------------------------------

   PagingStore getPagingStore();

   // To be called before the server is down
   void stop();

   /**
    * Save a snapshot of the current counter value in the journal
    */
   void counterSnapshot();

   void deleteCursorInfo();

   /**
    * This is a callback to inform the PageSubscription that something was routed, so the empty flag can be cleared
    */
   void bookmark(PagePosition position) throws Exception;

   PageSubscriptionCounter getCounter();


   boolean isCounterPending();

   long getId();

   boolean isPersistent();

   PageIterator iterator();

   PageIterator iterator(boolean browsing);


   void scheduleCleanupCheck();

   void cleanupEntries(boolean completeDelete) throws Exception;

   void onPageModeCleared(Transaction tx) throws Exception;

   void disableAutoCleanup();

   void enableAutoCleanup();

   void ack(PagedReference ref) throws Exception;

   boolean contains(PagedReference ref) throws Exception;

   boolean isAcked(PagedMessage pagedMessage);

   // for internal (cursor) classes
   void confirmPosition(PagePosition ref) throws Exception;

   void ackTx(Transaction tx, PagedReference position, boolean fromDelivery) throws Exception;

   default void ackTx(Transaction tx, PagedReference position) throws Exception {
      ackTx(tx, position, true);
   }
   // for internal (cursor) classes
   void confirmPosition(Transaction tx, PagePosition position, boolean fromDelivery) throws Exception;

   /**
    * {@return the first page in use or MAX_LONG if none is in use}
    */
   long getFirstPage();

   // Reload operations

   void reloadACK(PagePosition position);

   boolean reloadPageCompletion(PagePosition position) throws Exception;

   void reloadPageInfo(long pageNr);

   /**
    * To be called when the cursor decided to ignore a position.
    */
   void positionIgnored(PagePosition position);

   void lateDeliveryRollback(PagePosition position);

   /**
    * To be used to avoid a redelivery of a prepared ACK after load
    */
   void reloadPreparedACK(Transaction tx, PagePosition position);

   void processReload() throws Exception;

   void addPendingDelivery(PagedMessage pagedMessage);

   void redeliver(PageIterator iterator, PagedReference reference);

   void printDebug();

   boolean isComplete(long page);

   void forEachConsumedPage(Consumer<ConsumedPage> pageCleaner);

   /**
    * To be used to requery the reference
    */
   PagedMessage queryMessage(PagePosition pos);

   Queue getQueue();

   void onDeletePage(Page deletedPage) throws Exception;

   void removePendingDelivery(PagedMessage pagedMessage);

   ConsumedPage locatePageInfo(long pageNr);

   // ============ QueueMemoryManager methods ============

   @Override
   default boolean supportsDirectDelivery() {
      return true;
   }

   @Override
   default PageIterator pageIterator(boolean browsing) {
      return iterator(browsing);
   }

   @Override
   default boolean contains(MessageReference reference) throws Exception {
      if (reference.isPaged()) {
         return contains((PagedReference) reference);
      }
      return false;
   }

   @Override
   default void pageAckTx(Transaction tx, PagedReference ref) throws Exception {
      ackTx(tx, ref);
   }

   @Override
   default void pageAck(PagedReference ref) throws Exception {
      ack(ref);
   }

   @Override
   default PageSubscriptionCounter getPageCounter() {
      return getCounter();
   }
}
