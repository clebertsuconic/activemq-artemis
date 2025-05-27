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

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.AddressQueueStatus;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.config.AbstractPersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSettingJSON;
import org.apache.activemq.artemis.core.persistence.config.PersistedBridgeConfiguration;
import org.apache.activemq.artemis.core.persistence.config.PersistedConnector;
import org.apache.activemq.artemis.core.persistence.config.PersistedDivertConfiguration;
import org.apache.activemq.artemis.core.persistence.config.PersistedKeyValuePair;
import org.apache.activemq.artemis.core.persistence.config.PersistedRole;
import org.apache.activemq.artemis.core.persistence.config.PersistedSecuritySetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedUser;
import org.apache.activemq.artemis.core.persistence.impl.AbstractStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.persistence.impl.journal.JDBCJournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.StatementsManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.group.impl.GroupBinding;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;

public class ParallelDBStorageManager extends AbstractStorageManager {

   // TODO: provide configuration for this
   final int batchSize = 1000;

   final Configuration configuration;
   JDBCConnectionProvider connectionProvider;
   DatabaseStorageConfiguration databaseConfiguration;

   // the plan is to have many of these in a pool, for now while I bootstrap things I'm just having one
   StatementsManager statementsManager;

   public Configuration getConfig() {
      return journalDelegate.getConfig();
   }

   public StatementsManager getStatementsManager() {
      return statementsManager;
   }

   @Override
   public long getMaxRecordSize() {
      return journalDelegate.getMaxRecordSize();
   }

   @Override
   public long getWarningRecordSize() {
      return journalDelegate.getWarningRecordSize();
   }

   // we are (at the moment) still using the legacy journal for some tasks
   final JDBCJournalStorageManager journalDelegate;


   public ParallelDBStorageManager(Configuration configuration,
                                    CriticalAnalyzer analyzer,
                                    ExecutorFactory executorFactory,
                                    ExecutorFactory ioExecutorFactory,
                                    ScheduledExecutorService scheduledExecutorService) {
      super(analyzer, 1, executorFactory, scheduledExecutorService, ioExecutorFactory);
      this.configuration = configuration;
      this.journalDelegate = new JDBCJournalStorageManager(configuration, analyzer, executorFactory, ioExecutorFactory, scheduledExecutorService);
   }

   @Override
   public void start() throws Exception {
      this.databaseConfiguration = (DatabaseStorageConfiguration)configuration.getStoreConfiguration();
      this.connectionProvider = databaseConfiguration.getConnectionProvider();
      journalDelegate.start();
      initSchema();
   }

   @Override
   public void persistIdGenerator() {
      journalDelegate.persistIdGenerator();
   }

   @Override
   public boolean isStarted() {
      return journalDelegate.isStarted();
   }

   public JournalLoadInformation[] loadInternalOnly() throws Exception {
      return journalDelegate.loadInternalOnly();
   }

   @Override
   public Journal getMessageJournal() {
      return journalDelegate.getMessageJournal();
   }

   @Override
   public Journal getBindingsJournal() {
      return journalDelegate.getBindingsJournal();
   }

   @Override
   public boolean addToPage(PagingStore store, Message msg, Transaction tx, RouteContextList listCtx) throws Exception {
      return journalDelegate.addToPage(store, msg, tx, listCtx);
   }

   private void initSchema() throws Exception {
      String messagesTableName = databaseConfiguration.getParallelDBMessages();
      String referencesTableName = databaseConfiguration.getParallelDBReferences();
      try (Connection connection = connectionProvider.getConnection()) {
         JDBCUtils.createTable(connection, connectionProvider.getSQLProvider(), messagesTableName, connectionProvider.getSQLProvider().getCreateParallelDBMessages(messagesTableName));
         JDBCUtils.createTable(connection, connectionProvider.getSQLProvider(), referencesTableName, connectionProvider.getSQLProvider().getCreateParallelDBReferences(referencesTableName));

         // TODO: what is the best place for the time?
         statementsManager = new StatementsManager(scheduledExecutorService, executorFactory.getExecutor(), configuration.getJournalBufferTimeout_NIO(), databaseConfiguration, connectionProvider, batchSize);
         statementsManager.start();
      }
   }

   @Override
   public void criticalError(Throwable error) {
      journalDelegate.criticalError(error);
   }

   public IDGenerator getIDGenerator() {
      return journalDelegate.getIDGenerator();
   }

   @Override
   public long generateID() {
      return journalDelegate.generateID();
   }

   @Override
   public long getCurrentID() {
      return journalDelegate.getCurrentID();
   }

   @Override
   public void confirmPendingLargeMessageTX(Transaction tx, long messageID, long recordID) throws Exception {
      journalDelegate.confirmPendingLargeMessageTX(tx, messageID, recordID);
   }

   @Override
   public void confirmPendingLargeMessage(long recordID) throws Exception {
      journalDelegate.confirmPendingLargeMessage(recordID);
   }

   @Override
   public void storeMapRecord(long id,
                              byte recordType,
                              Persister persister,
                              Object record,
                              boolean sync,
                              IOCompletion completionCallback) throws Exception {
      journalDelegate.storeMapRecord(id, recordType, persister, record, sync, completionCallback);
   }

   @Override
   public void storeMapRecord(long id,
                              byte recordType,
                              Persister persister,
                              Object record,
                              boolean sync) throws Exception {
      journalDelegate.storeMapRecord(id, recordType, persister, record, sync);
   }

   @Override
   public void deleteMapRecord(long id, boolean sync) throws Exception {
      journalDelegate.deleteMapRecord(id, sync);
   }

   @Override
   public void deleteMapRecordTx(long txid, long id) throws Exception {
      journalDelegate.deleteMapRecordTx(txid, id);
   }

   @Override
   public void lineUpContext() {
      journalDelegate.lineUpContext();
   }

   @Override
   public void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
   }

   @Override
   public void storeMessage(Message message) throws Exception {
      new Exception("store message ").printStackTrace(System.out);
      statementsManager.storeMessage(message, null, getContext());
      statementsManager.flushTL();
   }

   @Override
   public void storeReference(long queueID, long messageID, boolean last) throws Exception {
      new Exception("storeRference " + queueID + ", message = " + messageID + ", last=" + last).printStackTrace(System.out);
      statementsManager.storeReference(messageID, queueID, null, getContext());
      if (last) {
         statementsManager.flushTL();
      }
      statementsManager.flushTL();
   }

   @Override
   public void writeLock() {
      journalDelegate.writeLock();
   }

   @Override
   public void writeUnlock() {
      journalDelegate.writeUnlock();
   }

   @Override
   public ArtemisCloseable closeableReadLock(boolean tryLock) {
      return journalDelegate.closeableReadLock(tryLock);
   }

   @Override
   public void deleteMessage(long messageID) throws Exception {

   }

   @Override
   public void updateScheduledDeliveryTime(MessageReference ref) throws Exception {
   }

   @Override
   public void storeDuplicateID(SimpleString address, byte[] duplID, long recordID) throws Exception {
      journalDelegate.storeDuplicateID(address, duplID, recordID);
   }

   @Override
   public void deleteDuplicateID(long recordID) throws Exception {
      journalDelegate.deleteDuplicateID(recordID);
   }

   @Override
   public void storeAcknowledge(long queueID, long messageID) throws Exception {

   }

   @Override
   public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception {
      journalDelegate.storeCursorAcknowledge(queueID, position);
   }

   @Override
   public void storeMessageTransactional(long txID, Message message) throws Exception {
      new Exception("store message transaction").printStackTrace(System.out);
      statementsManager.storeMessage(message, txID, getContext());
      statementsManager.flushTL();
   }

   @Override
   public void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception {
      journalDelegate.storePageTransaction(txID, pageTransaction);
   }

   @Override
   public void updatePageTransaction(long txID, PageTransactionInfo pageTransaction, int depages) throws Exception {
      journalDelegate.updatePageTransaction(txID, pageTransaction, depages);
   }

   @Override
   public void storeReferenceTransactional(long txID, long queueID, long messageID) throws Exception {
      statementsManager.storeReference(messageID, queueID, txID, getContext());
      statementsManager.flushTL();
   }

   @Override
   public void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception {
   }

   @Override
   public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception {
      journalDelegate.storeCursorAcknowledgeTransactional(txID, queueID, position);
   }

   @Override
   public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception {
      journalDelegate.storePageCompleteTransactional(txID, queueID, position);
   }

   @Override
   public void deletePageComplete(long ackID) throws Exception {
      journalDelegate.deletePageComplete(ackID);
   }

   @Override
   public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception {
      journalDelegate.deleteCursorAcknowledgeTransactional(txID, ackID);
   }

   @Override
   public void deleteCursorAcknowledge(long ackID) throws Exception {
      journalDelegate.deleteCursorAcknowledge(ackID);
   }

   @Override
   public long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception {
      return journalDelegate.storeHeuristicCompletion(xid, isCommit);
   }

   @Override
   public void deleteHeuristicCompletion(long id) throws Exception {
      journalDelegate.deleteHeuristicCompletion(id);
   }

   @Override
   public void deletePageTransactional(long recordID) throws Exception {
      journalDelegate.deletePageTransactional(recordID);
   }

   @Override
   public void updateScheduledDeliveryTimeTransactional(long txID, MessageReference ref) throws Exception {
      journalDelegate.updateScheduledDeliveryTimeTransactional(txID, ref);
   }

   @Override
   public void prepare(long txID, Xid xid) throws Exception {
      journalDelegate.prepare(txID, xid);
   }

   @Override
   public void commit(long txID) throws Exception {
      journalDelegate.commit(txID);
   }

   @Override
   public void commitBindings(long txID) throws Exception {
      journalDelegate.commitBindings(txID);
   }

   @Override
   public void rollbackBindings(long txID) throws Exception {
      journalDelegate.rollbackBindings(txID);
   }

   @Override
   public void commit(long txID, boolean lineUpContext) throws Exception {
      journalDelegate.commit(txID, lineUpContext);
   }

   @Override
   public void asyncCommit(long txID) throws Exception {
      journalDelegate.asyncCommit(txID);
   }

   @Override
   public void rollback(long txID) throws Exception {
      journalDelegate.rollback(txID);
   }

   @Override
   public void storeDuplicateIDTransactional(long txID,
                                             SimpleString address,
                                             byte[] duplID,
                                             long recordID) throws Exception {
      journalDelegate.storeDuplicateIDTransactional(txID, address, duplID, recordID);
   }

   @Override
   public void updateDuplicateIDTransactional(long txID,
                                              SimpleString address,
                                              byte[] duplID,
                                              long recordID) throws Exception {
      journalDelegate.updateDuplicateIDTransactional(txID, address, duplID, recordID);
   }

   @Override
   public void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception {
      journalDelegate.deleteDuplicateIDTransactional(txID, recordID);
   }

   @Override
   public void updateDeliveryCount(MessageReference ref) throws Exception {
      journalDelegate.updateDeliveryCount(ref);
   }

   @Override
   public void storeAddressSetting(PersistedAddressSettingJSON addressSetting) throws Exception {
      journalDelegate.storeAddressSetting(addressSetting);
   }

   @Override
   public List<AbstractPersistedAddressSetting> recoverAddressSettings() throws Exception {
      return journalDelegate.recoverAddressSettings();
   }

   @Override
   public AbstractPersistedAddressSetting recoverAddressSettings(SimpleString address) {
      return journalDelegate.recoverAddressSettings(address);
   }

   @Override
   public List<PersistedSecuritySetting> recoverSecuritySettings() throws Exception {
      return journalDelegate.recoverSecuritySettings();
   }

   @Override
   public void storeSecuritySetting(PersistedSecuritySetting persistedRoles) throws Exception {
      journalDelegate.storeSecuritySetting(persistedRoles);
   }

   @Override
   public void storeDivertConfiguration(PersistedDivertConfiguration persistedDivertConfiguration) throws Exception {
      journalDelegate.storeDivertConfiguration(persistedDivertConfiguration);
   }

   @Override
   public void deleteDivertConfiguration(String divertName) throws Exception {
      journalDelegate.deleteDivertConfiguration(divertName);
   }

   @Override
   public List<PersistedDivertConfiguration> recoverDivertConfigurations() {
      return journalDelegate.recoverDivertConfigurations();
   }

   @Override
   public void storeBridgeConfiguration(PersistedBridgeConfiguration persistedBridgeConfiguration) throws Exception {
      journalDelegate.storeBridgeConfiguration(persistedBridgeConfiguration);
   }

   @Override
   public void deleteBridgeConfiguration(String bridgeName) throws Exception {
      journalDelegate.deleteBridgeConfiguration(bridgeName);
   }

   @Override
   public List<PersistedBridgeConfiguration> recoverBridgeConfigurations() {
      return journalDelegate.recoverBridgeConfigurations();
   }

   @Override
   public void storeConnector(PersistedConnector persistedConnector) throws Exception {
      journalDelegate.storeConnector(persistedConnector);
   }

   @Override
   public void deleteConnector(String connectorName) throws Exception {
      journalDelegate.deleteConnector(connectorName);
   }

   @Override
   public List<PersistedConnector> recoverConnectors() {
      return journalDelegate.recoverConnectors();
   }

   @Override
   public void storeUser(PersistedUser persistedUser) throws Exception {
      journalDelegate.storeUser(persistedUser);
   }

   @Override
   public void deleteUser(String username) throws Exception {
      journalDelegate.deleteUser(username);
   }

   @Override
   public Map<String, PersistedUser> getPersistedUsers() {
      return journalDelegate.getPersistedUsers();
   }

   @Override
   public void storeRole(PersistedRole persistedRole) throws Exception {
      journalDelegate.storeRole(persistedRole);
   }

   @Override
   public void deleteRole(String username) throws Exception {
      journalDelegate.deleteRole(username);
   }

   @Override
   public Map<String, PersistedRole> getPersistedRoles() {
      return journalDelegate.getPersistedRoles();
   }

   @Override
   public void storeKeyValuePair(PersistedKeyValuePair persistedKeyValuePair) throws Exception {
      journalDelegate.storeKeyValuePair(persistedKeyValuePair);
   }

   @Override
   public void deleteKeyValuePair(String mapId, String key) throws Exception {
      journalDelegate.deleteKeyValuePair(mapId, key);
   }

   @Override
   public Map<String, PersistedKeyValuePair> getPersistedKeyValuePairs(String mapId) {
      return journalDelegate.getPersistedKeyValuePairs(mapId);
   }

   @Override
   public void storeID(long journalID, long id) throws Exception {
      journalDelegate.storeID(journalID, id);
   }

   @Override
   public void deleteID(long journalD) throws Exception {
      journalDelegate.deleteID(journalD);
   }

   @Override
   public void deleteAddressSetting(SimpleString addressMatch) throws Exception {
      journalDelegate.deleteAddressSetting(addressMatch);
   }

   @Override
   public void deleteSecuritySetting(SimpleString addressMatch) throws Exception {
      journalDelegate.deleteSecuritySetting(addressMatch);
   }

   @Override
   public JournalLoadInformation loadMessageJournal(PostOffice postOffice,
                                                    PagingManager pagingManager,
                                                    ResourceManager resourceManager,
                                                    Map<Long, QueueBindingInfo> queueInfos,
                                                    Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    Set<Pair<Long, Long>> pendingLargeMessages,
                                                    Set<Long> storedLargeMessages,
                                                    List<PageCountPending> pendingNonTXPageCounter,
                                                    JournalLoader journalLoader,
                                                    List<Consumer<RecordInfo>> journalRecordsListener) throws Exception {
      return journalDelegate.loadMessageJournal(postOffice, pagingManager, resourceManager, queueInfos, duplicateIDMap, pendingLargeMessages, storedLargeMessages, pendingNonTXPageCounter, journalLoader, journalRecordsListener);
   }

   public void checkInvalidPageTransactions(PagingManager pagingManager,
                                            Set<PageTransactionInfo> invalidPageTransactions) {
      journalDelegate.checkInvalidPageTransactions(pagingManager, invalidPageTransactions);
   }

   @Override
   public void addGrouping(GroupBinding groupBinding) throws Exception {
      journalDelegate.addGrouping(groupBinding);
   }

   @Override
   public void deleteGrouping(long tx, GroupBinding groupBinding) throws Exception {
      journalDelegate.deleteGrouping(tx, groupBinding);
   }

   @Override
   public void updateQueueBinding(long tx, Binding binding) throws Exception {
      journalDelegate.updateQueueBinding(tx, binding);
   }

   @Override
   public void addQueueBinding(long tx, Binding binding) throws Exception {
      journalDelegate.addQueueBinding(tx, binding);
   }

   @Override
   public void deleteQueueBinding(long tx, long queueBindingID) throws Exception {
      journalDelegate.deleteQueueBinding(tx, queueBindingID);
   }

   @Override
   public long storeQueueStatus(long queueID, AddressQueueStatus status) throws Exception {
      return journalDelegate.storeQueueStatus(queueID, status);
   }

   @Override
   public void deleteQueueStatus(long recordID) throws Exception {
      journalDelegate.deleteQueueStatus(recordID);
   }

   @Override
   public long storeAddressStatus(long addressID, AddressQueueStatus status) throws Exception {
      return journalDelegate.storeAddressStatus(addressID, status);
   }

   @Override
   public void deleteAddressStatus(long recordID) throws Exception {
      journalDelegate.deleteAddressStatus(recordID);
   }

   @Override
   public void addAddressBinding(long tx, AddressInfo addressInfo) throws Exception {
      journalDelegate.addAddressBinding(tx, addressInfo);
   }

   @Override
   public void deleteAddressBinding(long tx, long addressBindingID) throws Exception {
      journalDelegate.deleteAddressBinding(tx, addressBindingID);
   }

   @Override
   public long storePageCounterInc(long txID, long queueID, int value, long persistentSize) throws Exception {
      return journalDelegate.storePageCounterInc(txID, queueID, value, persistentSize);
   }

   @Override
   public long storePageCounterInc(long queueID, int value, long persistentSize) throws Exception {
      return journalDelegate.storePageCounterInc(queueID, value, persistentSize);
   }

   @Override
   public long storePageCounter(long txID, long queueID, long value, long persistentSize) throws Exception {
      return journalDelegate.storePageCounter(txID, queueID, value, persistentSize);
   }

   @Override
   public long storePendingCounter(long queueID, long pageID) throws Exception {
      return journalDelegate.storePendingCounter(queueID, pageID);
   }

   @Override
   public void deleteIncrementRecord(long txID, long recordID) throws Exception {
      journalDelegate.deleteIncrementRecord(txID, recordID);
   }

   @Override
   public void deletePageCounter(long txID, long recordID) throws Exception {
      journalDelegate.deletePageCounter(txID, recordID);
   }

   @Override
   public void deletePendingPageCounter(long txID, long recordID) throws Exception {
      journalDelegate.deletePendingPageCounter(txID, recordID);
   }

   @Override
   public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos,
                                                    List<GroupingInfo> groupingInfos,
                                                    List<AddressBindingInfo> addressBindingInfos) throws Exception {
      return journalDelegate.loadBindingJournal(queueBindingInfos, groupingInfos, addressBindingInfos);
   }

   @Override
   public void pageClosed(SimpleString address, long pageNumber) {

   }

   @Override
   public void pageDeleted(SimpleString address, long pageNumber) {

   }

   @Override
   public void pageWrite(SimpleString address,
                         PagedMessage message,
                         long pageNumber,
                         boolean storageUp,
                         boolean originallyReplicated) {

   }

   @Override
   public boolean waitOnOperations(long timeout) throws Exception {
      return false;
   }

   @Override
   public void waitOnOperations() throws Exception {

   }

   @Override
   public ByteBuffer allocateDirectBuffer(int size) {
      return null;
   }

   @Override
   public void freeDirectBuffer(ByteBuffer buffer) {

   }

   @Override
   public LargeServerMessage createCoreLargeMessage() {
      return null;
   }

   @Override
   public LargeServerMessage createCoreLargeMessage(long id, Message message) throws Exception {
      return null;
   }

   @Override
   public LargeServerMessage onLargeMessageCreate(long id, LargeServerMessage largeMessage) throws Exception {
      return null;
   }

   @Override
   public SequentialFile createFileForLargeMessage(long messageID, LargeMessageExtension extension) {
      return null;
   }

   @Override
   public void largeMessageClosed(LargeServerMessage largeServerMessage) throws ActiveMQException {

   }

   @Override
   public void deleteLargeMessageBody(LargeServerMessage largeServerMessage) throws ActiveMQException {

   }

   @Override
   public void startReplication(ReplicationManager replicationManager,
                                PagingManager pagingManager,
                                String nodeID,
                                boolean autoFailBack,
                                long initialReplicationSyncTimeout) throws Exception {

   }

   @Override
   public void stopReplication() {

   }

   @Override
   public void addBytesToLargeMessage(SequentialFile appendFile, long messageID, byte[] bytes) throws Exception {

   }

   @Override
   public void addBytesToLargeMessage(SequentialFile file, long messageId, ActiveMQBuffer bytes) throws Exception {

   }

   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {

   }

   @Override
   public void stop() throws Exception {

   }
}
