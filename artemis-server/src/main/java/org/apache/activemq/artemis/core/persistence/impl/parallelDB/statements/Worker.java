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

package org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.tasks.Task;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;

public class Worker implements Runnable {

   public Worker(JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseConfiguration, int batchSize) throws SQLException  {
      connection = connectionProvider.getConnection();
      connection.setAutoCommit(false);
      messageStatement = new MessageStatement(connection, connectionProvider, databaseConfiguration.getParallelDBMessages(), batchSize);
      referencesStatement = new ReferencesStatement(connection, connectionProvider, databaseConfiguration.getParallelDBReferences(), batchSize);
      txMessagesStatement = new UpdateTXStatement(connection, connectionProvider, databaseConfiguration.getParallelDBMessages(), batchSize);
      txReferencesStatement = new UpdateTXStatement(connection, connectionProvider, databaseConfiguration.getParallelDBReferences(), batchSize);
   }

   final Connection connection;
   public final MessageStatement messageStatement;
   public final ReferencesStatement referencesStatement;
   public final UpdateTXStatement txMessagesStatement;
   public final UpdateTXStatement txReferencesStatement;

   public void doRun(List<Task> taskList) throws SQLException {
      taskList.forEach(this::doStore);
      try {
         messageStatement.flushPending(false);
         referencesStatement.flushPending(false);
         txReferencesStatement.flushPending(false);
         txMessagesStatement.flushPending(false);
      } catch (SQLException e) {
         try {
            connection.rollback();
         } catch (Throwable ignored) {
         }
         throw e;
      }
      connection.commit();
      messageStatement.confirmData();
      referencesStatement.confirmData();
      txReferencesStatement.confirmData();
      txMessagesStatement.confirmData();
   }

   public void doStore(Task task) {
      task.store(this);
   }

   public void close() throws SQLException {
      connection.close();
   }

   @Override
   public void run() {

   }

}
