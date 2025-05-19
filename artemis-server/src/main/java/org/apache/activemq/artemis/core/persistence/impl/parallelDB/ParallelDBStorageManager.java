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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
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
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;

public class ParallelDBStorageManager extends AbstractStorageManager {

   // TODO: provide configuration for this
   final int batchSize = 1000;

   SQLProvider sqlProvider;
   DatabaseStorageConfiguration databaseConfiguration;
   JDBCConnectionProvider connectionProvider;

   // the plan is to have many of these in a pool, for now while I bootstrap things I'm just having one
   StatementsManager statementsManager;

   public ParallelDBStorageManager(CriticalAnalyzer analyzer,
                                   int numberOfPaths,
                                   ExecutorFactory executorFactory,
                                   ScheduledExecutorService scheduledExecutorService,
                                   ExecutorFactory ioExecutorFactory) {
      super(analyzer, numberOfPaths, executorFactory, scheduledExecutorService, ioExecutorFactory);
   }


   public void init(DatabaseStorageConfiguration databaseConfiguration) throws Exception {
      this.connectionProvider = databaseConfiguration.getConnectionProvider();
      this.databaseConfiguration = databaseConfiguration;
      initSchema(connectionProvider);
   }

   private void initSchema(JDBCConnectionProvider connectionProvider) throws Exception {
      String messagesTableName = databaseConfiguration.getParallelDBMessages();
      JDBCUtils.createTable(connectionProvider, messagesTableName, connectionProvider.getSQLProvider().getCreateParallelDBMessages(messagesTableName));
      statementsManager = new StatementsManager(databaseConfiguration, connectionProvider, batchSize);
      try (Connection connection = connectionProvider.getConnection()) {
      }
   }

   @Override
   public void criticalError(Throwable error) {

   }

   @Override
   public void lineUpContext() {

   }

   @Override
   public void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {

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
   public void confirmPendingLargeMessageTX(Transaction transaction, long messageID, long recordID) throws Exception {

   }

   @Override
   public void confirmPendingLargeMessage(long recordID) throws Exception {

   }

   @Override
   public void storeMessage(Message message) throws Exception {
      statementsManager.storeMessage(message, getContext());
   }

   @Override
   public void storeReference(long queueID, long messageID, boolean last) throws Exception {

   }

   @Override
   public void deleteMessage(long messageID) throws Exception {

   }

   @Override
   public void storeAcknowledge(long queueID, long messageID) throws Exception {

   }

   @Override
   public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception {

   }

   @Override
   public void updateDeliveryCount(MessageReference ref) throws Exception {

   }

   @Override
   public void updateScheduledDeliveryTime(MessageReference ref) throws Exception {

   }

   @Override
   public void storeDuplicateID(SimpleString address, byte[] duplID, long recordID) throws Exception {

   }

   @Override
   public void deleteDuplicateID(long recordID) throws Exception {

   }

   @Override
   public void storeMessageTransactional(long txID, Message message) throws Exception {

   }

   @Override
   public void storeReferenceTransactional(long txID, long queueID, long messageID) throws Exception {

   }

   @Override
   public void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception {

   }

   @Override
   public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception {

   }

   @Override
   public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception {

   }

   @Override
   public void deleteCursorAcknowledge(long ackID) throws Exception {

   }

   @Override
   public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception {

   }

   @Override
   public void deletePageComplete(long ackID) throws Exception {

   }

   @Override
   public void updateScheduledDeliveryTimeTransactional(long txID, MessageReference ref) throws Exception {

   }

   @Override
   public void storeDuplicateIDTransactional(long txID,
                                             SimpleString address,
                                             byte[] duplID,
                                             long recordID) throws Exception {

   }

   @Override
   public void updateDuplicateIDTransactional(long txID,
                                              SimpleString address,
                                              byte[] duplID,
                                              long recordID) throws Exception {

   }

   @Override
   public void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception {

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
   public void prepare(long txID, Xid xid) throws Exception {

   }

   @Override
   public void commit(long txID) throws Exception {

   }

   @Override
   public void commit(long txID, boolean lineUpContext) throws Exception {

   }

   @Override
   public void asyncCommit(long txID) throws Exception {

   }

   @Override
   public void rollback(long txID) throws Exception {

   }

   @Override
   public void rollbackBindings(long txID) throws Exception {

   }

   @Override
   public void commitBindings(long txID) throws Exception {

   }

   @Override
   public void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception {

   }

   @Override
   public void updatePageTransaction(long txID, PageTransactionInfo pageTransaction, int depage) throws Exception {

   }

   @Override
   public void deletePageTransactional(long recordID) throws Exception {

   }

   @Override
   public JournalLoadInformation loadMessageJournal(PostOffice postOffice,
                                                    PagingManager pagingManager,
                                                    ResourceManager resourceManager,
                                                    Map<Long, QueueBindingInfo> queueInfos,
                                                    Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    Set<Pair<Long, Long>> pendingLargeMessages,
                                                    Set<Long> largeMessagesInFolder,
                                                    List<PageCountPending> pendingNonTXPageCounter,
                                                    JournalLoader journalLoader,
                                                    List<Consumer<RecordInfo>> extraRecordsLoader) throws Exception {
      return null;
   }

   @Override
   public long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception {
      return 0;
   }

   @Override
   public void deleteHeuristicCompletion(long id) throws Exception {

   }

   @Override
   public void addQueueBinding(long tx, Binding binding) throws Exception {

   }

   @Override
   public void updateQueueBinding(long tx, Binding binding) throws Exception {

   }

   @Override
   public void deleteQueueBinding(long tx, long queueBindingID) throws Exception {

   }

   @Override
   public long storeQueueStatus(long queueID, AddressQueueStatus status) throws Exception {
      return 0;
   }

   @Override
   public void deleteQueueStatus(long recordID) throws Exception {

   }

   @Override
   public long storeAddressStatus(long addressID, AddressQueueStatus status) throws Exception {
      return 0;
   }

   @Override
   public void deleteAddressStatus(long recordID) throws Exception {

   }

   @Override
   public void addAddressBinding(long tx, AddressInfo addressInfo) throws Exception {

   }

   @Override
   public void deleteAddressBinding(long tx, long addressBindingID) throws Exception {

   }

   @Override
   public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos,
                                                    List<GroupingInfo> groupingInfos,
                                                    List<AddressBindingInfo> addressBindingInfos) throws Exception {
      return null;
   }

   @Override
   public void addGrouping(GroupBinding groupBinding) throws Exception {

   }

   @Override
   public void deleteGrouping(long tx, GroupBinding groupBinding) throws Exception {

   }

   @Override
   public void storeAddressSetting(PersistedAddressSettingJSON addressSetting) throws Exception {

   }

   @Override
   public void deleteAddressSetting(SimpleString addressMatch) throws Exception {

   }

   @Override
   public List<AbstractPersistedAddressSetting> recoverAddressSettings() throws Exception {
      return List.of();
   }

   @Override
   public AbstractPersistedAddressSetting recoverAddressSettings(SimpleString address) {
      return null;
   }

   @Override
   public void storeSecuritySetting(PersistedSecuritySetting persistedRoles) throws Exception {

   }

   @Override
   public void deleteSecuritySetting(SimpleString addressMatch) throws Exception {

   }

   @Override
   public List<PersistedSecuritySetting> recoverSecuritySettings() throws Exception {
      return List.of();
   }

   @Override
   public void storeDivertConfiguration(PersistedDivertConfiguration persistedDivertConfiguration) throws Exception {

   }

   @Override
   public void deleteDivertConfiguration(String divertName) throws Exception {

   }

   @Override
   public List<PersistedDivertConfiguration> recoverDivertConfigurations() {
      return List.of();
   }

   @Override
   public void storeBridgeConfiguration(PersistedBridgeConfiguration persistedBridgeConfiguration) throws Exception {

   }

   @Override
   public void deleteBridgeConfiguration(String bridgeName) throws Exception {

   }

   @Override
   public List<PersistedBridgeConfiguration> recoverBridgeConfigurations() {
      return List.of();
   }

   @Override
   public void storeConnector(PersistedConnector persistedConnector) throws Exception {

   }

   @Override
   public void deleteConnector(String connectorName) throws Exception {

   }

   @Override
   public List<PersistedConnector> recoverConnectors() {
      return List.of();
   }

   @Override
   public void storeUser(PersistedUser persistedUser) throws Exception {

   }

   @Override
   public void deleteUser(String username) throws Exception {

   }

   @Override
   public Map<String, PersistedUser> getPersistedUsers() {
      return Map.of();
   }

   @Override
   public void storeRole(PersistedRole persistedRole) throws Exception {

   }

   @Override
   public void deleteRole(String role) throws Exception {

   }

   @Override
   public Map<String, PersistedRole> getPersistedRoles() {
      return Map.of();
   }

   @Override
   public void storeKeyValuePair(PersistedKeyValuePair persistedKeyValuePair) throws Exception {

   }

   @Override
   public void deleteKeyValuePair(String mapId, String key) throws Exception {

   }

   @Override
   public Map<String, PersistedKeyValuePair> getPersistedKeyValuePairs(String mapId) {
      return Map.of();
   }

   @Override
   public long storePageCounter(long txID, long queueID, long value, long persistentSize) throws Exception {
      return 0;
   }

   @Override
   public long storePendingCounter(long queueID, long pageID) throws Exception {
      return 0;
   }

   @Override
   public void deleteIncrementRecord(long txID, long recordID) throws Exception {

   }

   @Override
   public void deletePageCounter(long txID, long recordID) throws Exception {

   }

   @Override
   public void deletePendingPageCounter(long txID, long recordID) throws Exception {

   }

   @Override
   public long storePageCounterInc(long txID, long queueID, int add, long persistentSize) throws Exception {
      return 0;
   }

   @Override
   public long storePageCounterInc(long queueID, int add, long size) throws Exception {
      return 0;
   }

   @Override
   public Journal getBindingsJournal() {
      return null;
   }

   @Override
   public Journal getMessageJournal() {
      return null;
   }

   @Override
   public void startReplication(ReplicationManager replicationManager,
                                PagingManager pagingManager,
                                String nodeID,
                                boolean autoFailBack,
                                long initialReplicationSyncTimeout) throws Exception {

   }

   @Override
   public boolean addToPage(PagingStore store, Message msg, Transaction tx, RouteContextList listCtx) throws Exception {
      return false;
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
   public void storeID(long journalID, long id) throws Exception {

   }

   @Override
   public void deleteID(long journalD) throws Exception {

   }

   @Override
   public ArtemisCloseable closeableReadLock(boolean tryLock) {
      return null;
   }

   @Override
   public void persistIdGenerator() {

   }

   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {

   }

   @Override
   public void storeMapRecord(long id,
                              byte recordType,
                              Persister persister,
                              Object record,
                              boolean sync,
                              IOCompletion completionCallback) throws Exception {

   }

   @Override
   public void storeMapRecord(long id,
                              byte recordType,
                              Persister persister,
                              Object record,
                              boolean sync) throws Exception {

   }

   @Override
   public void deleteMapRecord(long id, boolean sync) throws Exception {

   }

   @Override
   public void deleteMapRecordTx(long txid, long id) throws Exception {

   }

   @Override
   public void start() throws Exception {

   }

   @Override
   public void stop() throws Exception {

   }

   @Override
   public boolean isStarted() {
      return false;
   }

   @Override
   public long generateID() {
      return 0;
   }

   @Override
   public long getCurrentID() {
      return 0;
   }
}
