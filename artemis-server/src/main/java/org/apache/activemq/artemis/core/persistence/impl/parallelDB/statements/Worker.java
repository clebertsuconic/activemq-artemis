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

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.tasks.Task;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Runnable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final StatementsManager statementsManager;

   private final String name;

   public Worker(StatementsManager statementsManager, JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseConfiguration, int batchSize, String name) throws SQLException  {
      this.statementsManager = statementsManager;
      connection = connectionProvider.getConnection();
      connection.setAutoCommit(false);
      messageStatement = new MessageStatement(connection, connectionProvider, databaseConfiguration.getParallelDBMessages(), batchSize);
      referencesStatement = new ReferencesStatement(connection, connectionProvider, databaseConfiguration.getParallelDBReferences(), batchSize);
      txMessagesStatement = new UpdateTXStatement(connection, connectionProvider, databaseConfiguration.getParallelDBMessages(), batchSize);
      txReferencesStatement = new UpdateTXStatement(connection, connectionProvider, databaseConfiguration.getParallelDBReferences(), batchSize);
      this.name = name;
   }

   final Connection connection;
   public final MessageStatement messageStatement;
   public final ReferencesStatement referencesStatement;
   public final UpdateTXStatement txMessagesStatement;
   public final UpdateTXStatement txReferencesStatement;

   List<Task> taskList;

   public void setTaskList(List<Task> taskList) {
      this.taskList = taskList;
   }

   @Override
   public void run() {
      logger.info("Worker {} running with {} tasks", name, taskList.size());
      try {
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
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         messageStatement.clear();
         referencesStatement.clear();
         txReferencesStatement.clear();
         txMessagesStatement.clear();
         // TODO-important treat the exception with something like critical exception... or retries...
      } finally {
         this.taskList = null;
         statementsManager.workerDone(this);
      }
   }

   public void doStore(Task task) {
      task.store(this);
   }

   public void close() {
      try {
         connection.close();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

}
