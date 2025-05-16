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

package org.apache.activemq.artemis.core.persistence.impl.parallelDB;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.MessageStatement;
import org.apache.activemq.artemis.jdbc.parallelDB.BatchableStatement;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

public class SingleThreadDBManager {

   DatabaseStorageConfiguration databaseConfiguration;
   JDBCConnectionProvider connectionProvider;

   Connection connection;

   public SingleThreadDBManager(DatabaseStorageConfiguration databaseConfiguration, JDBCConnectionProvider connectionProvider) throws SQLException {
      this.databaseConfiguration = databaseConfiguration;
      this.connectionProvider = connectionProvider;
      init();
   }

   public void init() throws SQLException {
      connection = connectionProvider.getConnection();
      //SQLProvider sqlProvider = n
      //messageStatement = new MessageStatement(connection, )
   }

   MessageStatement messageStatement;
}
