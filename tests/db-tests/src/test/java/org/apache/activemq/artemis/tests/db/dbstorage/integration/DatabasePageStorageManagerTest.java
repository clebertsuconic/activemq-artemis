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
package org.apache.activemq.artemis.tests.db.dbstorage.integration;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.dbimpl.DatabasePage;
import org.apache.activemq.artemis.core.paging.dbimpl.DatabasePagingStoreFactory;
import org.apache.activemq.artemis.core.paging.dbimpl.DatabasePagingStoreImpl;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.db.dbstorage.statements.AbstractStatementTest;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.artemis.database.DatabaseProvider;
import org.mockito.Mockito;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class DatabasePageStorageManagerTest extends AbstractStatementTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @TestTemplate
   public void testWritePageDirect() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      long addressID = 1;
      long pageID = 1;
      int nrecords = 100;

      DatabasePage page = new DatabasePage(SimpleString.of("testAddress"), databaseStorageManager, pageID, addressID, databaseStorageManager.getDataManager());
      page.open(true);

      for (int i = 0; i < nrecords; i++) {
         CoreMessage message = new CoreMessage().initBuffer(1024).setDurable(true);
         message.setMessageID(i + 1);
         message.getBodyBuffer().writeByte((byte) 'Z');
         PagedMessageImpl pagedMessage = new PagedMessageImpl(message, new long[]{1});
         page.writeDirect(pagedMessage);
      }

      assertTrue(OperationContextImpl.getContext().waitCompletion(5000));

      try (Connection connection = databaseProvider.getConnection()) {
         assertEquals(nrecords, selectCount(connection, databaseProvider.getSqlProvider().getPage()));
      }
   }

   @TestTemplate
   public void testWriteAndDeletePage() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      long addressID = 1;
      int nrecordsPerPage = 50;

      DatabasePage pageToDelete = null;

      for (int pageID = 1; pageID <= 2; pageID++) {
         DatabasePage page = new DatabasePage(SimpleString.of("testAddress"), databaseStorageManager, pageID, addressID, databaseStorageManager.getDataManager());
         page.open(true);
         if (pageToDelete == null) {
            pageToDelete = page;
         }
         for (int i = 0; i < nrecordsPerPage; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1024).setDurable(true);
            message.setMessageID(pageID * 1000 + i + 1);
            message.getBodyBuffer().writeByte((byte) 'Z');
            PagedMessageImpl pagedMessage = new PagedMessageImpl(message, new long[]{1});
            page.writeDirect(pagedMessage);
         }
      }

      assertTrue(OperationContextImpl.getContext().waitCompletion(5000));

      try (Connection connection = databaseProvider.getConnection()) {
         assertEquals(nrecordsPerPage * 2, selectCount(connection, databaseProvider.getSqlProvider().getPage()));
         assertEquals(nrecordsPerPage * 2, selectCount(connection, databaseProvider.getSqlProvider().getPageRefs()));
      }

      pageToDelete.delete(null);

      assertTrue(OperationContextImpl.getContext().waitCompletion(5000));

      try (Connection connection = databaseProvider.getConnection()) {
         assertEquals(nrecordsPerPage, selectCount(connection, databaseProvider.getSqlProvider().getPage()));
         assertEquals(nrecordsPerPage, selectCount(connection, databaseProvider.getSqlProvider().getPageRefs()));
      }
   }

   @TestTemplate
   public void testWritePageThroughPagingStore() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      AddressInfo addressInfo = new AddressInfo("testAddress");
      addressInfo.setId(1);

      DatabasePagingStoreFactory factory = new DatabasePagingStoreFactory(databaseStorageManager, 100, scheduledExecutorService, executorFactory, false, address -> addressInfo);

      PagingManager pagingManager = Mockito.mock(PagingManager.class);
      DatabasePagingStoreImpl pagingStore = new DatabasePagingStoreImpl(SimpleString.of("testAddress"), scheduledExecutorService, 100, pagingManager, databaseStorageManager, factory, SimpleString.of("testAddress"), new AddressSettings(), executorFactory.getExecutor(), false, addressInfo);

      DatabasePage page = (DatabasePage) pagingStore.newPageObject(1);
      page.open(true);

      int nrecords = 50;

      for (int i = 0; i < nrecords; i++) {
         CoreMessage message = new CoreMessage().initBuffer(1024).setDurable(true);
         message.setMessageID(i + 1);
         message.getBodyBuffer().writeByte((byte) 'Z');
         PagedMessageImpl pagedMessage = new PagedMessageImpl(message, new long[]{1});
         page.writeDirect(pagedMessage);
      }

      assertTrue(OperationContextImpl.getContext().waitCompletion(5000));

      try (Connection connection = databaseProvider.getConnection()) {
         assertEquals(nrecords, selectCount(connection, databaseProvider.getSqlProvider().getPage()));
      }
   }
}
