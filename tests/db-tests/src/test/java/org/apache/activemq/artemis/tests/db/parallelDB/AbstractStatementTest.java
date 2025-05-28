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
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.tests.db.common.Database;
import org.apache.activemq.artemis.tests.db.common.ParameterDBTestBase;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class AbstractStatementTest extends ParameterDBTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Configuration configuration;
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
   public void setupTest() throws Exception {
      storageConfiguration = createDefaultDatabaseStorageConfiguration();
      this.configuration = createDefaultNettyConfig();
      this.configuration.setStoreConfiguration(storageConfiguration);

      executorService = Executors.newFixedThreadPool(5);
      runAfter(executorService::shutdownNow);

      scheduledExecutorService = Executors.newScheduledThreadPool(5);
      runAfter(scheduledExecutorService::shutdownNow);

      executorFactory = new OrderedExecutorFactory(executorService);
      criticalAnalyzer = Mockito.mock(CriticalAnalyzer.class);
   }



   public void checkMessageCounts(int messageCount) throws SQLException {
      try (Connection jdbcconnection = storageConfiguration.getConnectionProvider().getConnection()) {
         assertEquals(messageCount, selectCount(jdbcconnection, storageConfiguration.getParallelDBMessages()));
         assertEquals(messageCount, selectCount(jdbcconnection, storageConfiguration.getParallelDBReferences()));
      }
   }
}
