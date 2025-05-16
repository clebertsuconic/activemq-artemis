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
package org.apache.activemq.artemis.jdbc.store.drivers;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * Class to hold common database functionality such as drivers and connections
 */
@SuppressWarnings("SynchronizeOnNonFinalField")
public abstract class JDBCTableBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected SQLProvider sqlProvider;
   protected String tableName;

   protected JDBCConnectionProvider connectionProvider;

   public JDBCTableBase() {
   }

   public JDBCTableBase(JDBCConnectionProvider connectionProvider, String tableName) {
      this.connectionProvider = connectionProvider;
      this.sqlProvider = connectionProvider.getSQLProvider();
      this.tableName = tableName;
   }

   public void start() throws SQLException {
      createSchema();
      prepareStatements();
   }

   public void stop() throws SQLException {

   }

   protected abstract void prepareStatements();

   protected abstract void createSchema() throws SQLException;

   protected final void createTable(String... schemaSqls) throws SQLException {
      createTableIfNotExists(tableName, schemaSqls);
   }

   public void destroy() throws Exception {
      if (logger.isTraceEnabled()) {
         logger.trace("dropping {}", tableName, new Exception("trace"));
      }
      final String dropTableSql = "DROP TABLE " + tableName;
      try (Connection connection = connectionProvider.getConnection()) {
         try {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
               statement.executeUpdate(dropTableSql);
            }
            connection.commit();
         } catch (SQLException e) {
            logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e, dropTableSql).toString());
            try {
               connection.rollback();
            } catch (SQLException rollbackEx) {
               logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), rollbackEx, dropTableSql).toString());
               throw rollbackEx;
            }
            throw e;
         }
      }
   }

   private void createTableIfNotExists(String tableName, String... sqls) throws SQLException {
      JDBCUtils.createTableIfNotExists(connectionProvider, tableName, sqls);
   }

   public void setTableName(String tableName) {
      this.tableName = tableName;
   }

   public void setJdbcConnectionProvider(JDBCConnectionProvider connectionProvider) {
      this.connectionProvider = connectionProvider;
   }

   public JDBCConnectionProvider getJdbcConnectionProvider() {
      return this.connectionProvider;
   }

}
