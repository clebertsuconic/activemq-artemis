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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.artemis.database.DatabaseStoreTX;import org.apache.artemis.database.data.DBData;
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
import org.apache.artemis.database.statements.BatchableStatement;
import org.apache.artemis.database.DatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWorker extends DataAgent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public DataWorker(Consumer<DataWorker> onDone, DatabaseProvider databaseProvider, int batchSize, String name) throws SQLException  {
      super(databaseProvider);
      this.onDone = onDone;
      this.name = name;
      this.batchSize = batchSize;
      connect();
   }

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
   public ArrayList<DatabaseStoreTX> pendingTX;
   // To be called when the worker is done
   private final Consumer<DataWorker> onDone;

   @Override
   protected void connect() throws SQLException {
      super.connect();
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
      pendingTX = new ArrayList<>();
   }

   List<DBData> dataList;

   public void setTaskList(List<DBData> dataList) {
      this.dataList = dataList;
   }

   @Override
   protected void doCleanup() {
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
      onDone.accept(this);
   }

   @Override
   protected void doBeforeCommit() throws SQLException {
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

   @Override
   protected void doAfterCommit() {
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

   @Override
   protected void doError(Exception exception) {
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
