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
package org.apache.activemq.artemis.jdbc.store.sql;

public interface SQLProvider {

   long getMaxBlobSize();

   String[] getCreateJournalTableSQL(String tableName);

   String[] getCreateParallelDBMessages(String tableName);

   String getInsertJournalRecordsSQL(String tableName);

   String getSelectJournalRecordsSQL(String tableName);

   String getDeleteJournalRecordsSQL(String tableName);

   String getDeleteJournalTxRecordsSQL(String tableName);

   String[] getCreateFileTableSQL(String tableName);

   String getInsertFileSQL(String tableName);

   String getSelectFileNamesByExtensionSQL(String tableName);

   String getSelectFileByFileName(String tableName);

   String getReplaceLargeObjectSQL(String tableName);

   String getAppendToLargeObjectSQL(String tableName);

   String getReadLargeObjectSQL(String tableName);

   String getDeleteFileSQL(String tableName);

   String getUpdateFileNameByIdSQL(String tableName);

   String getCopyFileRecordByIdSQL(String tableName);

   String getDropFileTableSQL(String tableName);

   String getCloneFileRecordByIdSQL(String tableName);

   String getCountJournalRecordsSQL(String tableName);

   boolean closeConnectionOnShutdown(String tableName);

   String createNodeManagerStoreTableSQL(String tableName);

   String createStateSQL(String tableName);

   String createNodeIdSQL(String tableName);

   String createPrimaryLockSQL(String tableName);

   String createBackupLockSQL(String tableName);

   String tryAcquirePrimaryLockSQL(String tableName);

   String tryAcquireBackupLockSQL(String tableName);

   String tryReleasePrimaryLockSQL(String tableName);

   String tryReleaseBackupLockSQL(String tableName);

   String isPrimaryLockedSQL(String tableName);

   String isBackupLockedSQL(String tableName);

   String renewPrimaryLockSQL(String tableName);

   String renewBackupLockSQL(String tableName);

   String currentTimestampSQL(String tableName);

   String currentTimestampTimeZoneId();

   String writeStateSQL(String tableName);

   String readStateSQL(String tableName);

   String writeNodeIdSQL(String tableName);

   String initializeNodeIdSQL(String tableName);

   String readNodeIdSQL(String tableName);

   String applyCase(String tableName);

   String getInsertPDBMessages(String tableName);

   interface Factory {
      SQLProvider create();
   }
}
