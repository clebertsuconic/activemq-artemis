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

package org.apache.activemq.artemis.core.persistence.impl.parallelDB.worker;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.dbdata.DBData;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.DeleteMessageStatement;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.DeleteReferenceStatement;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.InsertMessageStatement;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.ReferencesStatement;
import org.apache.activemq.artemis.jdbc.parallelDB.BatchableStatement;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWorker implements Runnable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public DataWorker(DataManager statementsManager, JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseConfiguration, int batchSize, String name) throws SQLException  {
      this.statementsManager = statementsManager;
      this.connectionProvider = connectionProvider;
      this.databaseConfiguration = databaseConfiguration;
      this.name = name;
      this.batchSize = batchSize;
      connect();
   }

   private final DataManager statementsManager;
   private final String name;
   Connection connection;
   public JDBCConnectionProvider connectionProvider;
   public DatabaseStorageConfiguration databaseConfiguration;
   int batchSize;

   public InsertMessageStatement insertMessageStatement;
   public ReferencesStatement referencesStatement;
   public DeleteReferenceStatement deleteReferenceStatement;
   public DeleteMessageStatement deleteMessageStatement;

   private List<BatchableStatement<?>> batchableStatements = new ArrayList<>();

   private void connect() throws SQLException {
      connection = connectionProvider.getConnection();
      connection.setAutoCommit(false);
      insertMessageStatement = new InsertMessageStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      referencesStatement = new ReferencesStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      deleteReferenceStatement = new DeleteReferenceStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      deleteMessageStatement = new DeleteMessageStatement(connection, connectionProvider, databaseConfiguration, batchSize);
   }

   List<DBData> dataList;

   public void setTaskList(List<DBData> dataList) {
      this.dataList = dataList;
   }

   @Override
   public void run() {
      logger.info("Worker {} running with {} tasks", name, dataList.size());
      try {
         for (int success = 0, retryI = 0; retryI < 5 && success == 0; retryI++) {
            dataList.forEach(this::doStore);
            try {
               insertMessageStatement.flushPending(false);
               referencesStatement.flushPending(false);
               deleteReferenceStatement.flushPending(false);
               deleteMessageStatement.flushPending(false);
               connection.commit();
               success++;
            } catch (SQLException e) {
               logger.warn("Retrying Connection:: {}", e.getMessage(), e);
               try {
                  connection.rollback();
                  connection.close();
               } catch (Throwable ignored) {
               }

               connect();
            }
         }
         insertMessageStatement.confirmData();
         referencesStatement.confirmData();
         deleteReferenceStatement.confirmData();
         deleteMessageStatement.confirmData();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         insertMessageStatement.clear();
         referencesStatement.clear();
         deleteReferenceStatement.clear();
         deleteMessageStatement.clear();
         // TODO-important treat the exception with something like critical exception... or retries...
      } finally {
         this.dataList = null;
         statementsManager.workerDone(this);
      }
   }

   public void doStore(DBData data) {
      data.store(this);
   }

   public void close() {
      try {
         connection.close();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

}
