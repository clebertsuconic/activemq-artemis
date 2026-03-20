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

package org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.DeleteMessageData;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteMessageStatement extends BatchableStatement<DeleteMessageData> {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public DeleteMessageStatement(Connection connection, JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseStorageConfiguration, int expectedSize) throws SQLException {
      super(connectionProvider, connection, getSQL(connectionProvider, databaseStorageConfiguration), expectedSize);
   }

   private static String getSQL(JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseStorageConfiguration) {
      String tableName = databaseStorageConfiguration.getParallelDBMessages();
      String sql = connectionProvider.getSQLProvider().getDeletePDBMessages(tableName);
      if (sql == null) {
         sql = "DELETE FROM " + tableName + " where MESSAGE_ID=?";
      }
      return sql;
   }

   @Override
   protected void doOne(DeleteMessageData task) throws Exception {
      preparedStatement.setLong(1, task.messageID);
   }


}
