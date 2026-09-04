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
package org.apache.activemq.artemis.tests.db.dbstorage.statements;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.artemis.database.data.DeleteAllPageRefData;
import org.apache.artemis.database.data.PageRefData;
import org.apache.artemis.database.data.DeletePageRefData;
import org.apache.artemis.database.statements.DeleteAllPageRefStatement;
import org.apache.artemis.database.statements.InsertPageRefStatement;
import org.apache.artemis.database.statements.DeletePageRefStatement;
import org.apache.activemq.artemis.tests.db.dbstorage.CountDownCompletion;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.artemis.database.DatabaseProvider;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class PageReferenceStatementTest extends AbstractStatementTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @TestTemplate
   public void testInsertPageRefDirectly() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      int nrecords = 100;

      CountDownCompletion latch = new CountDownCompletion(nrecords);

      try (Connection connection = databaseProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertPageRefStatement insertPageRefStatement = new InsertPageRefStatement(databaseProvider, connection, nrecords);
         for (int i = 1; i <= nrecords; i++) {
            PageRefData task = new PageRefData(1, 1, i, 1, latch);
            insertPageRefStatement.addElement(task, latch);
         }
         insertPageRefStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "DB_PAGE_REFERENCES"));
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @TestTemplate
   public void testInsertAndDeletePageRef() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      int nrecords = 50;

      CountDownCompletion insertLatch = new CountDownCompletion(nrecords);

      try (Connection connection = databaseProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertPageRefStatement insertPageRefStatement = new InsertPageRefStatement(databaseProvider, connection, nrecords);
         for (int i = 1; i <= nrecords; i++) {
            PageRefData task = new PageRefData(1, 1, i, 1, insertLatch);
            insertPageRefStatement.addElement(task, insertLatch);
         }
         insertPageRefStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "DB_PAGE_REFERENCES"));

         int recordsToDelete = 20;
         CountDownCompletion deleteLatch = new CountDownCompletion(recordsToDelete);
         DeletePageRefStatement deletePageRefStatement = new DeletePageRefStatement(databaseProvider, connection, recordsToDelete);
         for (int i = 1; i <= recordsToDelete; i++) {
            DeletePageRefData task = new DeletePageRefData(1, 1, i, 1, deleteLatch);
            deletePageRefStatement.addElement(task, deleteLatch);
         }
         deletePageRefStatement.flushPending(true);

         assertEquals(nrecords - recordsToDelete, selectCount(connection, "DB_PAGE_REFERENCES"));
      }

      assertTrue(insertLatch.await(10, TimeUnit.SECONDS));
   }

   @TestTemplate
   public void testDeleteAllPageReferences() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      int nrecords = 50;

      CountDownCompletion insertLatch = new CountDownCompletion(nrecords);

      try (Connection connection = databaseProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertPageRefStatement insertPageRefStatement = new InsertPageRefStatement(databaseProvider, connection, nrecords);
         for (int i = 1; i <= nrecords; i++) {
            // Half on page 1, half on page 2 across different queue IDs
            long pageID = (i <= 25) ? 1 : 2;
            PageRefData task = new PageRefData(1, pageID, i, i % 3, insertLatch);
            insertPageRefStatement.addElement(task, insertLatch);
         }
         insertPageRefStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "DB_PAGE_REFERENCES"));

         CountDownCompletion deleteLatch = new CountDownCompletion(1);
         DeleteAllPageRefStatement deleteAllPageRefStatement = new DeleteAllPageRefStatement(databaseProvider, connection, 1);
         deleteAllPageRefStatement.addElement(new DeleteAllPageRefData(1, 1, deleteLatch), deleteLatch);
         deleteAllPageRefStatement.flushPending(true);

         // All 25 references for page 1 should be gone, leaving 25 references for page 2
         assertEquals(25, selectCount(connection, "DB_PAGE_REFERENCES"));
      }

      assertTrue(insertLatch.await(10, TimeUnit.SECONDS));
   }
}
