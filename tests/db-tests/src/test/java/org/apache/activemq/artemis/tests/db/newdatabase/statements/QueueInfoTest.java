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
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.NewDatabaseStorageManager;
import org.apache.activemq.artemis.core.transaction.impl.BindingsTransactionImpl;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.QueueData;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.queries.QueueJDBCQuery;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.tests.db.newdatabase.CountDownCompletion;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class QueueInfoTest extends AbstractStatementTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @TestTemplate
   public void testQueueInfoDirectly() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration, criticalAnalyzer, executorFactory, executorFactory, scheduledExecutorService, executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 50;

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 0; i < nrecords; i++) {
            BindingsTransactionImpl tx = new BindingsTransactionImpl(newDatabaseStorageManager);

            newDatabaseStorageManager.getDataManager().storeQueue(tx.getStorageTx(), 1, i + 1, "test" + i, "select from nothing" + i, RoutingType.MULTICAST, newDatabaseStorageManager.getContext());
            newDatabaseStorageManager.commitBindings(tx);
         }

         CountDownCompletion completion = new CountDownCompletion(1);
         newDatabaseStorageManager.getContext().executeOnCompletion(completion);
         assertTrue(completion.await(10, TimeUnit.SECONDS));

         assertEquals(nrecords, selectCount(connection, "QUEUE_INFO"));
      }

   }


   @TestTemplate
   public void testQueueInfoStorageManager() throws Exception {
      NewDatabaseStorageManager newDatabaseStorageManager = new NewDatabaseStorageManager(configuration, criticalAnalyzer, executorFactory, executorFactory, scheduledExecutorService, executorService);
      newDatabaseStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 50;

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 0; i < nrecords; i++) {
            BindingsTransactionImpl tx = new BindingsTransactionImpl(newDatabaseStorageManager);

            newDatabaseStorageManager.getDataManager().storeQueue(tx.getStorageTx(), 1, i + 1, "test" + i, "select from nothing" + i, RoutingType.ANYCAST, newDatabaseStorageManager.getContext());
            newDatabaseStorageManager.commitBindings(tx);
         }
         CountDownCompletion completion = new CountDownCompletion(1);
         newDatabaseStorageManager.getContext().executeOnCompletion(completion);
         assertTrue(completion.await(10, TimeUnit.SECONDS));

         QueueJDBCQuery query = new QueueJDBCQuery(connection);
         ArrayList<QueueData> queueData = new ArrayList<>();
         query.query(queueData::add);
         assertEquals(nrecords, queueData.size());
         queueData.forEach(d -> {
            assertEquals(RoutingType.ANYCAST, d.toQueueConfiguration().getRoutingType());
         });
      }
   }

}