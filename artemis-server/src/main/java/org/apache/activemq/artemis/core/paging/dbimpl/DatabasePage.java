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
package org.apache.activemq.artemis.core.paging.dbimpl;

import java.io.IOException;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.artemis.database.DatabaseStoreTX;
import org.apache.artemis.database.worker.DataManager;
import org.apache.activemq.artemis.utils.collections.EmptyList;
import org.apache.activemq.artemis.utils.collections.LinkedList;

/**
 * Database-backed {@link Page} implementation for use with the new database storage path.
 * <p>
 * Unlike {@link org.apache.activemq.artemis.core.paging.impl.FilePage}, this implementation stores
 * page data directly in the database rather than on the file system. Replication is not supported
 * for database-backed pages.
 */
public class DatabasePage extends Page {

   private final long addressID;
   private final DataManager dataManager;

   public DatabasePage(final SimpleString storeName,
                       final StorageManager storageManager,
                       final long pageId,
                       final long addressID,
                       final DataManager dataManager) {
      super(storeName, storageManager, pageId);
      this.addressID = addressID;
      this.dataManager = dataManager;
   }

   @Override
   public boolean storageExists() throws Exception {
      // TODO: query database to check whether this page's rows exist
      return false;
   }

   @Override
   public long storageSize() throws Exception {
      // TODO: return size of stored page data from database
      return getSize();
   }

   @Override
   public boolean open(boolean createFile) throws Exception {
      // No file handle to open; database connection is managed externally
      return true;
   }

   @Override
   public synchronized void close(boolean sendReplicaClose, boolean waitSync) throws Exception {
      if (sendReplicaClose && storageManager != null) {
         storageManager.pageClosed(storeName, getPageId());
      }
      // No file handle to close
   }

   @Override
   public boolean delete(final LinkedList<PagedMessage> messages) throws Exception {
      if (storageManager != null) {
         storageManager.pageDeleted(storeName, getPageId());
      }

      dataManager.deletePageReferences(addressID, getPageId(), storageManager.getContext());
      dataManager.deletePage(addressID, getPageId(), storageManager.getContext());

      usageExhaust();
      return true;
   }

   @Override
   public synchronized LinkedList<PagedMessage> read(StorageManager storage, boolean onlyLargeMessages) throws Exception {
      // TODO: read page messages from database
      return EmptyList.getEmptyList();
   }

   @Override
   public void writeDirect(PagedMessage message) throws Exception {
      DatabaseStoreTX databaseStoreTX = (DatabaseStoreTX) message.getStorageTX();
      Transaction localTx = null;
      if (databaseStoreTX == null) {
         localTx = new TransactionImpl(storageManager);
         databaseStoreTX = (DatabaseStoreTX) localTx.getStorageTx();
         localTx.setContainsPersistent();
      }
      addMessageToCache(message);
      dataManager.storePage(databaseStoreTX, addressID, getPageId(), message.getMessageNumber(), message.getMessage().getMessageID(), () -> encodeMessage(message.getMessage()), message.getTransactionID() > 0 ? message.getTransactionID() : null, storageManager.getContext());
      long[] queueIDs = message.getQueueIDs();
      if (queueIDs != null) {
         for (long queueID : queueIDs) {
            dataManager.storePageRef(databaseStoreTX, addressID, getPageId(), message.getMessageNumber(), queueID, storageManager.getContext());
         }
      }
      if (localTx != null) {
         localTx.commit();
      }
   }

   @Override
   public void sync() throws Exception {
      // No-op: database writes are committed through the DataManager
   }

   @Override
   public void trySync() throws IOException {
      // No-op
   }

   @Override
   public boolean isOpen() {
      return true;
   }

   @Override
   public int readNumberOfMessages() throws Exception {
      // TODO: count page rows from database
      return 0;
   }

   private static ActiveMQBuffer encodeMessage(Message message) {
      int size = message.getPersister().getEncodeSize(message);
      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(size);
      message.getPersister().encode(buffer, message);
      return buffer;
   }

   @Override
   public String toString() {
      return "DatabasePage::pageNr=" + getPageId() + ", storeName=" + storeName;
   }
}
