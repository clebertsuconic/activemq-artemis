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
package org.apache.activemq.artemis.tests.db.parallelDB;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.ParallelDBStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.MessageStatement;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.tests.db.common.Database;
import org.apache.activemq.artemis.tests.db.common.ParameterDBTestBase;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class BatchStatementTest extends ParameterDBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   DatabaseStorageConfiguration storageConfiguration;

   ExecutorService executorService;

   ScheduledExecutorService scheduledExecutorService;

   ExecutorFactory executorFactory;

   CriticalAnalyzer criticalAnalyzer;

   @Parameters(name = "db={0}")
   public static Collection<Object[]> parameters() {
      List<Database> dbList = Database.selectedList();
      dbList.remove(Database.DERBY); // no derby on this test

      return convertParameters(dbList);
   }

   // Used in @DisabledIf on class, avoids no-params failure with only -PDB-derby-tests
   public static boolean isNoDatabaseSelected() {
      return parameters().isEmpty();
   }

   @BeforeEach
   @Override
   public void setUp() throws Exception {
      super.setUp();
      assumeTrue(database != Database.DERBY);
      dropDatabase();
   }

   @Override
   protected final String getJDBCClassName() {
      return database.getDriverClass();
   }

   @BeforeEach
   public void setupTest() {
      storageConfiguration = createDefaultDatabaseStorageConfiguration();

      executorService = Executors.newFixedThreadPool(5);
      runAfter(executorService::shutdownNow);

      scheduledExecutorService = Executors.newScheduledThreadPool(5);
      runAfter(scheduledExecutorService::shutdownNow);

      executorFactory = new OrderedExecutorFactory(executorService);
      criticalAnalyzer = Mockito.mock(CriticalAnalyzer.class);
   }

   @TestTemplate
   public void testInit() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(criticalAnalyzer, 1, executorFactory, scheduledExecutorService, executorFactory);
      parallelDBStorageManager.init(storageConfiguration);
   }

   @TestTemplate
   public void testStoreMessageOnBatchableStatement() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(criticalAnalyzer, 1, executorFactory, scheduledExecutorService, executorFactory);
      parallelDBStorageManager.init(storageConfiguration);

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownLatch latch = new CountDownLatch(nrecords);

      IOCallback ioCallback = new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      };

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         MessageStatement messageStatement = new MessageStatement(connection, connectionProvider, storageConfiguration.getParallelDBMessages(), 100);
         for (int i = 1; i <= 100; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            messageStatement.addData(message, ioCallback);
         }
         messageStatement.flushPending();

         assertEquals(100, selectCount(connection, "ART_MESSAGES"));
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }


   @TestTemplate
   public void testTreatExceptionOnError() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(criticalAnalyzer, 1, executorFactory, scheduledExecutorService, executorFactory);
      parallelDBStorageManager.init(storageConfiguration);

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      AtomicInteger errors = new AtomicInteger(0);

      IOCallback ioCallback = new IOCallback() {
         @Override
         public void done() {
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
            errors.incrementAndGet();
         }
      };

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         MessageStatement messageStatement = new MessageStatement(connection, connectionProvider, storageConfiguration.getParallelDBMessages(), 100);
         for (int i = 1; i <= 100; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(1); // everything should fail with a DuplicateException
            message.getBodyBuffer().writeByte((byte) 'Z');
            messageStatement.addData(message, ioCallback);
         }
         assertThrows(SQLException.class, () -> messageStatement.flushPending());

         // forcing a commit, even though it failed... it should not commit any success
         connection.commit();

         assertEquals(0, selectCount(connection, "ART_MESSAGES"));
      }

      assertEquals(nrecords, errors.get());

   }



   private static int selectCount(Connection connection, String tableName) throws SQLException {
      try (Statement queryStatement = connection.createStatement()) {
         ResultSet rset = queryStatement.executeQuery("SELECT COUNT(*) FROM " + tableName);
         rset.next();
         return rset.getInt(1);
      }
   }
}