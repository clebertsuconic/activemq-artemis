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

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.impl.AbstractPagingStoreImpl;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

/**
 * Database-backed {@link org.apache.activemq.artemis.core.paging.PagingStore} implementation.
 * Creates {@link DatabasePage} instances for page storage.
 */
public class DatabasePagingStoreImpl extends AbstractPagingStoreImpl {

   private final AddressInfo addressInfo;

   public DatabasePagingStoreImpl(final SimpleString address,
                                  final ScheduledExecutorService scheduledExecutor,
                                  final long syncTimeout,
                                  final PagingManager pagingManager,
                                  final StorageManager storageManager,
                                  final PagingStoreFactory storeFactory,
                                  final SimpleString storeName,
                                  final AddressSettings addressSettings,
                                  final ArtemisExecutor executor,
                                  final boolean syncNonTransactional,
                                  final AddressInfo addressInfo) {
      this(address, scheduledExecutor, syncTimeout, pagingManager,
            storageManager, storeFactory,
            storeName, addressSettings, executor, syncNonTransactional,
            () -> false, addressInfo);
   }

   public DatabasePagingStoreImpl(final SimpleString address,
                                  final ScheduledExecutorService scheduledExecutor,
                                  final long syncTimeout,
                                  final PagingManager pagingManager,
                                  final StorageManager storageManager,
                                  final PagingStoreFactory storeFactory,
                                  final SimpleString storeName,
                                  final AddressSettings addressSettings,
                                  final ArtemisExecutor executor,
                                  final boolean syncNonTransactional,
                                  final Supplier<Boolean> purgePageFolder,
                                  final AddressInfo addressInfo) {
      super(address, scheduledExecutor, syncTimeout, pagingManager,
            storageManager, storeFactory,
            storeName, addressSettings, executor, syncNonTransactional,
            purgePageFolder);
      this.addressInfo = addressInfo;
   }

   public AddressInfo getAddressInfo() {
      return addressInfo;
   }

   @Override
   public boolean checkPageFileExists(final long pageNumber) {
      // TODO: query database to check whether this page exists
      return false;
   }

   @Override
   public Page newPageObject(final long pageNumber) throws Exception {
      return new DatabasePage(getStoreName(), getStorageManager(), pageNumber, addressInfo.getId(), ((DatabaseStorageManager) getStorageManager()).getDataManager());
   }
}
