/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.db.common;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.utils.RealServerTestBase;

public class DBTestBase extends RealServerTestBase {


   protected DatabaseStorageConfiguration createDBConfig(Database database) {
      DatabaseStorageConfiguration dbStorageConfiguration = new DatabaseStorageConfiguration();
      String connectionURI = database.getJdbcURI();
      dbStorageConfiguration.setJdbcDriverClassName(database.getDriverClass());
      dbStorageConfiguration.setJdbcConnectionUrl(connectionURI);
      dbStorageConfiguration.setBindingsTableName("BINDINGS");
      dbStorageConfiguration.setMessageTableName("MESSAGES");
      dbStorageConfiguration.setLargeMessageTableName("LARGE_MESSAGES");
      dbStorageConfiguration.setPageStoreTableName("PAGE_STORE");
      dbStorageConfiguration.setJdbcPassword(getJDBCPassword());
      dbStorageConfiguration.setJdbcUser(getJDBCUser());
      dbStorageConfiguration.setJdbcLockAcquisitionTimeoutMillis(getJdbcLockAcquisitionTimeoutMillis());
      dbStorageConfiguration.setJdbcLockExpirationMillis(getJdbcLockExpirationMillis());
      dbStorageConfiguration.setJdbcLockRenewPeriodMillis(getJdbcLockRenewPeriodMillis());
      dbStorageConfiguration.setJdbcNetworkTimeout(-1);
      dbStorageConfiguration.setJdbcAllowedTimeDiff(250L);
      return dbStorageConfiguration;
   }


   // Register the database on driver and prepares the classLoader to be used
   protected void registerDB(Database database) throws Exception {
      ClassLoader dbClassLoader = database.getDBClassLoader();
      if (dbClassLoader != null) {
         ClassLoader tccl = Thread.currentThread().getContextClassLoader();
         Thread.currentThread().setContextClassLoader(dbClassLoader);
         final Thread currentThread = Thread.currentThread();
         runAfter((() -> {
            currentThread.setContextClassLoader(tccl);
         }));
         database.registerDriver();
      }
   }


}
