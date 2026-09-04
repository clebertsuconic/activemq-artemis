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

package org.apache.artemis.database.worker;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.artemis.database.DatabaseStoreTX;
import org.apache.artemis.database.data.DBData;
import org.apache.artemis.database.queries.MessagesPendingDeliverQueryForUpdate;
import org.apache.artemis.database.statements.DeleteAddressStatement;
import org.apache.artemis.database.statements.DeleteMessageStatement;
import org.apache.artemis.database.statements.DeleteAllPageRefStatement;
import org.apache.artemis.database.statements.DeletePageRefStatement;
import org.apache.artemis.database.statements.DeletePageStatement;
import org.apache.artemis.database.statements.DeleteReferenceStatement;
import org.apache.artemis.database.statements.InsertAddressStatement;
import org.apache.artemis.database.statements.InsertMessageStatement;
import org.apache.artemis.database.statements.InsertPageRefStatement;
import org.apache.artemis.database.statements.InsertPageStatement;
import org.apache.artemis.database.statements.InsertQueueStatement;
import org.apache.artemis.database.statements.UpdateQueueStatement;
import org.apache.artemis.database.statements.DeleteQueueStatement;
import org.apache.artemis.database.statements.InsertGenericDataStatement;
import org.apache.artemis.database.statements.UpdateGenericDataStatement;
import org.apache.artemis.database.statements.DeleteGenericDataStatement;
import org.apache.artemis.database.statements.InsertReferencesStatement;
import org.apache.artemis.database.DatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWorker implements Runnable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final DatabaseProvider databaseProvider;
   protected Connection connection;

   private final String name;
   int batchSize;

   public InsertMessageStatement insertMessageStatement;
   public InsertReferencesStatement insertReferencesStatement;
   public DeleteReferenceStatement deleteReferenceStatement;
   public DeleteMessageStatement deleteMessageStatement;
   public DeleteAddressStatement deleteAddressStatement;
   public InsertAddressStatement insertAddressStatement;
   public InsertQueueStatement insertQueueStatement;
   public UpdateQueueStatement updateQueueStatement;
   public DeleteQueueStatement deleteQueueStatement;
   public InsertPageStatement insertPageStatement;
   public DeletePageStatement deletePageStatement;
   public InsertPageRefStatement insertPageRefStatement;
   public DeletePageRefStatement deletePageRefStatement;
   public DeleteAllPageRefStatement deleteAllPageRefStatement;
   public InsertGenericDataStatement insertGenericDataStatement;
   public UpdateGenericDataStatement updateGenericDataStatement;
   public DeleteGenericDataStatement deleteGenericDataStatement;
   public InsertGenericDataStatement insertBindingsGenericDataStatement;
   public UpdateGenericDataStatement updateBindingsGenericDataStatement;
   public DeleteGenericDataStatement deleteBindingsGenericDataStatement;
   public MessagesPendingDeliverQueryForUpdate pendingDeliveryQueryForUpdate;
   public ArrayList<DatabaseStoreTX> pendingTX;
   // To be called when the worker is done
   private final DataManager dataManager;

   public DataWorker(DataManager dataManager, DatabaseProvider databaseProvider, int batchSize, String name) throws SQLException {
      this.databaseProvider = databaseProvider;
      this.dataManager = dataManager;
      this.name = name;
      this.batchSize = batchSize;
      connect();
   }

   protected void connect() throws SQLException {
      connection = databaseProvider.getConnection();
      connection.setAutoCommit(false);
      insertMessageStatement = new InsertMessageStatement(databaseProvider, connection, batchSize);
      insertReferencesStatement = new InsertReferencesStatement(databaseProvider, connection, batchSize);
      deleteReferenceStatement = new DeleteReferenceStatement(databaseProvider, connection, batchSize);
      deleteMessageStatement = new DeleteMessageStatement(databaseProvider, connection, batchSize);
      deleteAddressStatement = new DeleteAddressStatement(databaseProvider, connection, batchSize);
      insertAddressStatement = new InsertAddressStatement(databaseProvider, connection, batchSize);
      insertQueueStatement = new InsertQueueStatement(databaseProvider, connection, batchSize);
      updateQueueStatement = new UpdateQueueStatement(databaseProvider, connection, batchSize);
      deleteQueueStatement = new DeleteQueueStatement(databaseProvider, connection, batchSize);
      insertPageStatement = new InsertPageStatement(databaseProvider, connection, batchSize);
      deletePageStatement = new DeletePageStatement(databaseProvider, connection, batchSize);
      insertPageRefStatement = new InsertPageRefStatement(databaseProvider, connection, batchSize);
      deletePageRefStatement = new DeletePageRefStatement(databaseProvider, connection, batchSize);
      deleteAllPageRefStatement = new DeleteAllPageRefStatement(databaseProvider, connection, batchSize);
      insertGenericDataStatement = new InsertGenericDataStatement(databaseProvider, connection, batchSize);
      updateGenericDataStatement = new UpdateGenericDataStatement(databaseProvider, connection, batchSize);
      deleteGenericDataStatement = new DeleteGenericDataStatement(databaseProvider, connection, batchSize);
      String bindingsTable = databaseProvider.getSqlProvider().getConfigData();
      insertBindingsGenericDataStatement = new InsertGenericDataStatement(databaseProvider, connection, batchSize, bindingsTable);
      updateBindingsGenericDataStatement = new UpdateGenericDataStatement(databaseProvider, connection, batchSize, bindingsTable);
      deleteBindingsGenericDataStatement = new DeleteGenericDataStatement(databaseProvider, connection, batchSize, bindingsTable);
      pendingDeliveryQueryForUpdate = new MessagesPendingDeliverQueryForUpdate(databaseProvider, connection);
      pendingDeliveryQueryForUpdate.prepare();
      pendingTX = new ArrayList<>();
   }

   List<DBData> dataList;

   public void setTaskList(List<DBData> dataList) {
      this.dataList = dataList;
   }

   SQLException executeWithRetry(SQLConsumer<DataWorker> action) {
      SQLException lastException = null;
      for (int retryI = 0; retryI < dataManager.getMaxRetries(); retryI++) {
         try {
            if (retryI > 0) {
               logger.info("Retrying SQL Action after a a SQL Exception", lastException);
               connect();
            }
            action.accept(this);
            return null;
         } catch (SQLException e) {
            logger.warn(e.getMessage(), e);
            lastException = e;
            disconnect(e);
         }
      }
      return new SQLException("Failed after " + dataManager.getMaxRetries() + " retries", lastException);
   }

   @Override
   public void run() {
      try {
         SQLException retryException = executeWithRetry(w -> doBeforeCommit());
         if (retryException == null) {
            // the commit could fail on the way back from the database after the data was already written,
            // so we cannot safely retry here. A failure at this point triggers a critical error.
            connection.commit();
            doAfterCommit();
         } else {
            doError(retryException);
            dataManager.criticalError(retryException);
         }
      } catch (Exception e) {
         dataManager.criticalError(e);
      } finally {
         doCleanup();
      }
   }

   protected void disconnect(SQLException e) {
      logger.warn("Retrying Connection:: {}", e.getMessage(), e);
      try {
         connection.rollback();
      } catch (Throwable ignored) {
      }

      try {
         connection.close();
      } catch (Throwable ignored) {
      }
   }

   private void doBeforeCommit() throws SQLException {
      logger.info("Worker {} running with {} tasks", name, dataList.size());
      dataList.forEach(this::doStore);
      insertMessageStatement.flushPending(false);
      insertReferencesStatement.flushPending(false);
      deleteReferenceStatement.flushPending(false);
      deleteMessageStatement.flushPending(false);
      deleteAddressStatement.flushPending(false);
      insertAddressStatement.flushPending(false);
      insertQueueStatement.flushPending(false);
      updateQueueStatement.flushPending(false);
      deleteQueueStatement.flushPending(false);
      insertPageStatement.flushPending(false);
      deletePageStatement.flushPending(false);
      insertPageRefStatement.flushPending(false);
      deletePageRefStatement.flushPending(false);
      deleteAllPageRefStatement.flushPending(false);
      insertGenericDataStatement.flushPending(false);
      updateGenericDataStatement.flushPending(false);
      deleteGenericDataStatement.flushPending(false);
      insertBindingsGenericDataStatement.flushPending(false);
      updateBindingsGenericDataStatement.flushPending(false);
      deleteBindingsGenericDataStatement.flushPending(false);
   }

   private void doAfterCommit() {
      insertMessageStatement.confirmData();
      insertReferencesStatement.confirmData();
      deleteReferenceStatement.confirmData();
      deleteMessageStatement.confirmData();
      insertReferencesStatement.confirmData();
      deleteAddressStatement.confirmData();
      insertAddressStatement.confirmData();
      insertQueueStatement.confirmData();
      updateQueueStatement.confirmData();
      deleteQueueStatement.confirmData();
      insertPageStatement.confirmData();
      deletePageStatement.confirmData();
      insertPageRefStatement.confirmData();
      deletePageRefStatement.confirmData();
      deleteAllPageRefStatement.confirmData();
      insertGenericDataStatement.confirmData();
      updateGenericDataStatement.confirmData();
      deleteGenericDataStatement.confirmData();
      insertBindingsGenericDataStatement.confirmData();
      updateBindingsGenericDataStatement.confirmData();
      deleteBindingsGenericDataStatement.confirmData();
      pendingTX.forEach(DatabaseStoreTX::completeIO);
   }

   private void doError(Exception exception) {
      insertMessageStatement.onError(exception);
      insertReferencesStatement.onError(exception);
      deleteReferenceStatement.onError(exception);
      deleteMessageStatement.onError(exception);
      insertReferencesStatement.onError(exception);
      deleteAddressStatement.onError(exception);
      insertAddressStatement.onError(exception);
      insertQueueStatement.onError(exception);
      updateQueueStatement.onError(exception);
      deleteQueueStatement.onError(exception);
      insertPageStatement.onError(exception);
      deletePageStatement.onError(exception);
      insertPageRefStatement.onError(exception);
      deletePageRefStatement.onError(exception);
      deleteAllPageRefStatement.onError(exception);
      insertGenericDataStatement.onError(exception);
      updateGenericDataStatement.onError(exception);
      deleteGenericDataStatement.onError(exception);
      insertBindingsGenericDataStatement.onError(exception);
      updateBindingsGenericDataStatement.onError(exception);
      deleteBindingsGenericDataStatement.onError(exception);
      // TODO: Critical Error
   }

   private void doCleanup() {
      this.dataList = null;
      insertMessageStatement.clear();
      insertReferencesStatement.clear();
      deleteReferenceStatement.clear();
      deleteMessageStatement.clear();
      deleteAddressStatement.clear();
      insertAddressStatement.clear();
      insertQueueStatement.clear();
      updateQueueStatement.clear();
      deleteQueueStatement.clear();
      insertPageStatement.clear();
      deletePageStatement.clear();
      insertPageRefStatement.clear();
      deletePageRefStatement.clear();
      deleteAllPageRefStatement.clear();
      insertGenericDataStatement.clear();
      updateGenericDataStatement.clear();
      deleteGenericDataStatement.clear();
      insertBindingsGenericDataStatement.clear();
      updateBindingsGenericDataStatement.clear();
      deleteBindingsGenericDataStatement.clear();
      pendingTX.clear();
      dataManager.workerDone(this);
   }

   public void doStore(DBData data) {
      data.perform(this);
   }

   public void close() {
      try {
         connection.close();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

}
