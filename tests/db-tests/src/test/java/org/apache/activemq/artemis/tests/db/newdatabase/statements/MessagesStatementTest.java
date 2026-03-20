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
package org.apache.activemq.artemis.tests.db.newdatabase.statements;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.NewDatabaseStorageManager;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.queries.MessagesJDBCQuery;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.InsertMessageStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.InsertReferencesStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.MessageData;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.MessageReferenceData;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.tests.db.newdatabase.CountDownCompletion;
import org.apache.activemq.artemis.tests.db.newdatabase.VariableCountCompletion;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class MessagesStatementTest extends AbstractStatementTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @TestTemplate
   public void testReferencesDirectly() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration,
                                                                           criticalAnalyzer,
                                                                           executorFactory,
                                                                           executorFactory,
                                                                           scheduledExecutorService,
                                                                           executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownCompletion latch = new CountDownCompletion(nrecords);

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertReferencesStatement insertReferencesStatement = new InsertReferencesStatement(connection, connectionProvider, storageConfiguration, 100);
         for (int i = 1; i <= nrecords; i++) {
            MessageReferenceData task = newDatabaseStorageManager.getDataManager().newReferenceTask(i, 1, i % 2 == 0 ? (long)i : null, latch);
            insertReferencesStatement.addData(task, latch);
         }
         insertReferencesStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "ART_REFERENCES"));
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @TestTemplate
   public void testMessagesDirectly() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration,
                                                                           criticalAnalyzer,
                                                                           executorFactory,
                                                                           executorFactory,
                                                                           scheduledExecutorService,
                                                                           executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownCompletion latch = new CountDownCompletion(nrecords);

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertMessageStatement insertMessageStatement = new InsertMessageStatement(connection, connectionProvider, storageConfiguration, 100);
         for (int i = 1; i <= nrecords; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            MessageData task = newDatabaseStorageManager.getDataManager().newMessageTask(message, null, latch);
            insertMessageStatement.addData(task, latch);
         }
         insertMessageStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "ART_MESSAGES"));
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @TestTemplate
   public void testMessagesStorageManager() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration,
                                                                           criticalAnalyzer,
                                                                           executorFactory,
                                                                           executorFactory,
                                                                           scheduledExecutorService,
                                                                           executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      Connection connection = connectionProvider.getConnection();
      runAfter(connection::close);

      int nrecords = 100;

      OperationContext context = newDatabaseStorageManager.getContext();
      runAfter(OperationContextImpl::clearContext);

      for (int i = 1; i <= nrecords; i++) {
         CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
         message.setMessageID(i);
         message.getBodyBuffer().writeByte((byte) 'Z');

         if (i % 2 == 0) {
            newDatabaseStorageManager.storeMessage(message);
         } else {
            TransactionImpl tx = new TransactionImpl(newDatabaseStorageManager);
            newDatabaseStorageManager.storeMessageTransactional(tx, message);
            newDatabaseStorageManager.commit(tx);
         }
      }

      assertTrue(context.waitCompletion(5000));

      assertEquals(nrecords, selectCount(connection, storageConfiguration.getParallelDBMessages()));

      int recordsToDelete = 20;

      for (int i = 1; i <= recordsToDelete; i++) {
         newDatabaseStorageManager.deleteMessage(i);
      }

      assertTrue(context.waitCompletion(5000));
      assertEquals(nrecords - recordsToDelete, selectCount(connection, storageConfiguration.getParallelDBMessages()));
   }

   @TestTemplate
   public void testQueryStuff() throws Exception {

      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration,
                                                                           criticalAnalyzer,
                                                                           executorFactory,
                                                                           executorFactory,
                                                                           scheduledExecutorService,
                                                                           executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      Connection connection = connectionProvider.getConnection();
      runAfter(connection::close);

      int nrecords = 100;

      OperationContext context = newDatabaseStorageManager.getContext();
      runAfter(OperationContextImpl::clearContext);

      for (int i = 1; i <= nrecords; i++) {
         CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
         message.setMessageID(i);
         message.putStringProperty("test", "t" + i);
         message.getBodyBuffer().writeByte((byte) 'Z');
         newDatabaseStorageManager.storeMessage(message);
      }

      assertTrue(context.waitCompletion(5000));

      MessagesJDBCQuery query = new MessagesJDBCQuery(connection);
      query.query(m -> logger.info("message {}", m));

   }


   @TestTemplate
   public void testMessagesAckTX() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration,
                                                                           criticalAnalyzer,
                                                                           executorFactory,
                                                                           executorFactory,
                                                                           scheduledExecutorService,
                                                                           executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      Connection connection = connectionProvider.getConnection();
      runAfter(connection::close);

      int nrecords = 1000;

      OperationContext context = newDatabaseStorageManager.getContext();
      runAfter(OperationContextImpl::clearContext);

      TransactionImpl tx = new TransactionImpl(newDatabaseStorageManager);
      for (int i = 1; i <= nrecords; i++) {
         newDatabaseStorageManager.storeReferenceTransactional(tx, 1, i);
      }
      newDatabaseStorageManager.commit(tx);

      assertTrue(context.waitCompletion(5000));

      assertEquals(nrecords, selectCount(connection, storageConfiguration.getParallelDBReferences()));

      tx = new TransactionImpl(newDatabaseStorageManager);
      for (int i = 1; i <= nrecords; i++) {
         newDatabaseStorageManager.storeAcknowledgeTransactional(tx, 1, i);
      }
      newDatabaseStorageManager.commit(tx);
      assertTrue(context.waitCompletion(5000));

      assertEquals(0, selectCount(connection, storageConfiguration.getParallelDBMessages()));
   }




   @TestTemplate
   public void testMessagesReferencesStorageManager() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration,
                                                                           criticalAnalyzer,
                                                                           executorFactory,
                                                                           executorFactory,
                                                                           scheduledExecutorService,
                                                                           executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      Connection connection = connectionProvider.getConnection();
      runAfter(connection::close);

      int nrecords = 100;

      OperationContext context = newDatabaseStorageManager.getContext();
      runAfter(OperationContextImpl::clearContext);

      for (int i = 1; i <= nrecords; i++) {
         if (i % 2 == 0) {
            newDatabaseStorageManager.storeReference(1, i, true);
         } else {
            TransactionImpl txID = new TransactionImpl(newDatabaseStorageManager);
            newDatabaseStorageManager.storeReferenceTransactional(txID, 1, i);
            newDatabaseStorageManager.commit(txID);
         }
      }

      assertTrue(context.waitCompletion(5000));

      assertEquals(nrecords, selectCount(connection, storageConfiguration.getParallelDBReferences()));


      int recordsToDelete = 20;

      for (int i = 1; i <= recordsToDelete; i++) {
         if (i % 2 == 1) {
            newDatabaseStorageManager.storeAcknowledge(1, i);
         } else {
            TransactionImpl txID = new TransactionImpl(newDatabaseStorageManager);
            newDatabaseStorageManager.storeAcknowledgeTransactional(txID, 1, i);
            newDatabaseStorageManager.commit(txID);
         }
      }

      assertTrue(context.waitCompletion(5000));

      assertEquals(nrecords - recordsToDelete, selectCount(connection, storageConfiguration.getParallelDBReferences()));

   }



   @TestTemplate
   public void testTreatExceptionOnError() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration,
                                                                           criticalAnalyzer,
                                                                           executorFactory,
                                                                           executorFactory,
                                                                           scheduledExecutorService,
                                                                           executorService);
      newDatabaseStorageManager.start();


      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;


      VariableCountCompletion ioCallback = new VariableCountCompletion();

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertMessageStatement insertMessageStatement = new InsertMessageStatement(connection, connectionProvider, storageConfiguration, 100);
         for (int i = 1; i <= nrecords; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(1); // everything should fail with a DuplicateException
            message.getBodyBuffer().writeByte((byte) 'Z');

            insertMessageStatement.addData(newDatabaseStorageManager.getDataManager().newMessageTask(message, null, ioCallback), ioCallback);
         }
         assertThrows(SQLException.class, () -> insertMessageStatement.flushPending(true));

         // forcing a commit, even though it failed... it should not commit any success
         connection.commit();

         assertEquals(0, selectCount(connection, "ART_MESSAGES"));
         assertEquals(0, ioCallback.errors.get());
      }
   }

}