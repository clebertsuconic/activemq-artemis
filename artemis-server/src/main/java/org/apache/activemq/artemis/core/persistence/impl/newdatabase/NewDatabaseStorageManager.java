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

package org.apache.activemq.artemis.core.persistence.impl.newdatabase;

import javax.transaction.xa.Xid;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.memory.AddressMemoryManager;
import org.apache.activemq.artemis.core.memory.GlobalMemoryManager;
import org.apache.activemq.artemis.core.paging.PageTransactionInfo;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.AddressQueueStatus;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.StorageTX;
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
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentAddressBindingEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentQueueBindingEncoding;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.queries.AddressJDBCQuery;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.queries.MessagesJDBCQuery;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.queries.QueueJDBCQuery;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.queries.ReferencesJDBCQuery;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.worker.DataManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
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
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewDatabaseStorageManager extends AbstractStorageManager {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   // TODO: provide configuration for this
   final int batchSize = 1000;

   final Executor executorService;
   final Configuration configuration;
   JDBCConnectionProvider connectionProvider;
   DatabaseStorageConfiguration databaseConfiguration;

   // the plan is to have many of these in a pool, for now while I bootstrap things I'm just having one
   DataManager dataManager;

   public Configuration getConfig() {
      return journalDelegate.getConfig();
   }

   public DataManager getDataManager() {
      return dataManager;
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


   public NewDatabaseStorageManager(Configuration configuration,
                                    CriticalAnalyzer analyzer,
                                    ExecutorFactory executorFactory,
                                    ExecutorFactory ioExecutorFactory,
                                    ScheduledExecutorService scheduledExecutorService,
                                    Executor executorService) {
      super(analyzer, 1, executorFactory, scheduledExecutorService, ioExecutorFactory);
      this.configuration = configuration;
      this.executorService = executorService;
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
   public boolean addToPage(AddressMemoryManager store, Message msg, Transaction tx, RouteContextList listCtx) throws Exception {
      return false;
   }

   private void initSchema() throws Exception {
      String messagesTableName = databaseConfiguration.getParallelDBMessages();
      String referencesTableName = databaseConfiguration.getParallelDBReferences();
      // TODO configure these
      String addressInfoTableName = "ADDRESS_INFO";
      String queueInfoTableName = "QUEUE_INFO";

      try (Connection connection = connectionProvider.getConnection()) {
         JDBCUtils.createTable(connection, connectionProvider.getSQLProvider(), messagesTableName, connectionProvider.getSQLProvider().getCreateParallelDBMessages(messagesTableName));
         JDBCUtils.createTable(connection, connectionProvider.getSQLProvider(), referencesTableName, connectionProvider.getSQLProvider().getCreateParallelDBReferences(referencesTableName));
         JDBCUtils.createTable(connection, connectionProvider.getSQLProvider(), addressInfoTableName, connectionProvider.getSQLProvider().getCreateParallelDBAddress(addressInfoTableName));
         JDBCUtils.createTable(connection, connectionProvider.getSQLProvider(), queueInfoTableName, connectionProvider.getSQLProvider().getCreateParallelDBQueue(queueInfoTableName));

         // TODO-IMPORTANT: what is the best place for the time?
         logger.info("Timeout:: {}", configuration.getJournalBufferTimeout_NIO());
         dataManager = new DataManager(scheduledExecutorService, executorFactory.getExecutor(), executorService, configuration.getJournalBufferTimeout_NIO(), databaseConfiguration, connectionProvider, batchSize);
         dataManager.start();
      }
   }

   @Override
   public StorageTX generateTX(long tx) {
      return new NewDatabaseStoreTX(tx);
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
   public void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
   }

   @Override
   public void storeMessage(Message message) throws Exception {
      dataManager.storeMessage(message, null, getContext());
   }

   @Override
   public void storeReference(long queueID, long messageID, boolean last) throws Exception {
      dataManager.storeReference(messageID, queueID, null, getContext());
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
      dataManager.deleteMessage(messageID, getContext());
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
      dataManager.ackMessage(queueID, messageID, getContext());
   }

   @Override
   public void storeAcknowledgeTransactional(Transaction tx, long queueID, long messageID) throws Exception {
      dataManager.ackMessage(tx.getStorageTx(), tx.getID(), queueID, messageID, getContext());
   }

   @Override
   public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception {
      journalDelegate.storeCursorAcknowledge(queueID, position);
   }

   @Override
   public void storeMessageTransactional(Transaction tx, Message message) throws Exception {
      dataManager.storeMessage(tx.getStorageTx(), message, tx.getID(), getContext());
   }

   @Override
   public void storePageTransaction(Transaction tx, PageTransactionInfo pageTransaction) throws Exception {
      journalDelegate.storePageTransaction(tx, pageTransaction);
   }

   @Override
   public void updatePageTransaction(Transaction tx, PageTransactionInfo pageTransaction, int depages) throws Exception {
      journalDelegate.updatePageTransaction(tx, pageTransaction, depages);
   }

   @Override
   public void storeReferenceTransactional(Transaction tx, long queueID, long messageID) throws Exception {
      dataManager.storeReference(tx.getStorageTx(), messageID, queueID, tx.getID(), getContext());
   }

   @Override
   public void deletePendingLargeMessage(long recordID) throws Exception {
      journalDelegate.deletePendingLargeMessage(recordID);
   }

   @Override
   public void storeCursorAcknowledgeTransactional(Transaction tx, long queueID, PagePosition position) throws Exception {
      journalDelegate.storeCursorAcknowledgeTransactional(tx, queueID, position);
   }

   @Override
   public void storePageCompleteTransactional(Transaction tx, long queueID, PagePosition position) throws Exception {
      journalDelegate.storePageCompleteTransactional(tx, queueID, position);
   }

   @Override
   public void deletePageComplete(long ackID) throws Exception {
      journalDelegate.deletePageComplete(ackID);
   }

   @Override
   public void deleteCursorAcknowledgeTransactional(Transaction tx, long ackID) throws Exception {
      journalDelegate.deleteCursorAcknowledgeTransactional(tx, ackID);
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
   public void updateScheduledDeliveryTimeTransactional(Transaction tx, MessageReference ref) throws Exception {
      journalDelegate.updateScheduledDeliveryTimeTransactional(tx, ref);
   }

   @Override
   public void prepare(Transaction tx, Xid xid) throws Exception {
      journalDelegate.prepare(tx, xid);
   }

   @Override
   public void commit(Transaction tx) throws Exception {
      commit(tx, true);
   }

   @Override
   public void commitBindings(Transaction tx) throws Exception {
      if (tx.getStorageTx() != null && !tx.getStorageTx().isEmpty()) {
         /*OperationContext context = getContext();
         context.storeLineUp();
         tx.getStorageTx().setContext(context); */
         dataManager.storeTX(tx.getStorageTx());
      } else {
         journalDelegate.commitBindings(tx);
      }

   }

   @Override
   public void rollbackBindings(Transaction tx) throws Exception {
      journalDelegate.rollbackBindings(tx);
   }

   @Override
   public void commit(Transaction tx, boolean lineUpContext) throws Exception {

      if (tx.getStorageTx() == null || tx.getStorageTx().isEmpty()) {
         // TODO-NOW: this is for the proof of concept where we still delegate to the old journal,
         // this thing should go before merged into the main branch
         journalDelegate.commit(tx, lineUpContext);
      } else {
         OperationContext context = getContext();
         context.storeLineUp();
         tx.getStorageTx().setContext(context);
         dataManager.storeTX(tx.getStorageTx());
      }
   }

   @Override
   public void asyncCommit(Transaction tx) throws Exception {
      journalDelegate.asyncCommit(tx);
   }

   @Override
   public void rollback(Transaction tx) throws Exception {
      journalDelegate.rollback(tx);
   }

   @Override
   public void storeDuplicateIDTransactional(Transaction tx,
                                             SimpleString address,
                                             byte[] duplID,
                                             long recordID) throws Exception {
      journalDelegate.storeDuplicateIDTransactional(tx, address, duplID, recordID);
   }

   @Override
   public void updateDuplicateIDTransactional(Transaction tx,
                                              SimpleString address,
                                              byte[] duplID,
                                              long recordID) throws Exception {
      journalDelegate.updateDuplicateIDTransactional(tx, address, duplID, recordID);
   }

   @Override
   public void deleteDuplicateIDTransactional(Transaction tx, long recordID) throws Exception {
      journalDelegate.deleteDuplicateIDTransactional(tx, recordID);
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
   public DivertConfiguration getDivertConfiguration(String name) {
      return journalDelegate.getDivertConfiguration(name);
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


   // TODO-IMPORTANT: Once we do a proper implementation of the new JDBC Manager, this should go away
   // This is an extension point for the new JDBC manager to reload messages
   // it should go away once we finish implementation
   public void loadMessage(Map<Long, Message> loadMessages) throws Exception {
   }

   // TODO-IMPORTANT: Once we do a proper implementation of the new JDBC Manager, this should go away
   // This is an extension point for the new JDBC manager to reload messages
   // it should go away once we finish implementation
   public void loadReferences(Map<Long, Message> loadMessages, JournalLoader loader) throws Exception {
   }



   @Override
   public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos,
                                                    List<GroupingInfo> groupingInfos,
                                                    List<AddressBindingInfo> addressBindingInfos) throws Exception {
      try (Connection connection = this.connectionProvider.getConnection()) {
         AddressJDBCQuery addressQuery = new AddressJDBCQuery(connection);
         addressQuery.query(data -> {
            // TODO: add internal and auto-create to the query
            PersistentAddressBindingEncoding info = new PersistentAddressBindingEncoding();
            info.setId(data.id);
            info.setName(SimpleString.of(data.address));

            if (data.isMulticast) {
               info.getRoutingTypes().add(RoutingType.MULTICAST);
            }

            if (data.isAnycast) {
               info.getRoutingTypes().add(RoutingType.ANYCAST);
            }

            addressBindingInfos.add(info);
         });

         QueueJDBCQuery queueQuery = new QueueJDBCQuery(connection);
         queueQuery.query(data -> {
            // TODO: add internal and auto-create to the query
            QueueConfiguration queueConfiguration = data.toQueueConfiguration();
            PersistentQueueBindingEncoding queueBindingEncoding = new PersistentQueueBindingEncoding(queueConfiguration);
            queueBindingInfos.add(queueBindingEncoding);
         });


      }

      return journalDelegate.loadBindingJournal(queueBindingInfos, groupingInfos, addressBindingInfos);
   }

   @Override
   public JournalLoadInformation loadMessageJournal(PostOffice postOffice,
                                                    GlobalMemoryManager pagingManager,
                                                    ResourceManager resourceManager,
                                                    Map<Long, QueueBindingInfo> queueInfos,
                                                    Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    Set<Pair<Long, Long>> pendingLargeMessages,
                                                    Set<Long> storedLargeMessages,
                                                    List<PageCountPending> pendingNonTXPageCounter,
                                                    JournalLoader journalLoader,
                                                    List<Consumer<RecordInfo>> journalRecordsListener) throws Exception {

      Map<Long, Message> loadedMessages = new HashMap<>();
      try (Connection connection = this.connectionProvider.getConnection()) {
         MessagesJDBCQuery query = new MessagesJDBCQuery(connection);
         query.query(data -> {
            loadedMessages.put(data.message.getMessageID(), data.message);
         });

         ReferencesJDBCQuery referencesQuery = new ReferencesJDBCQuery(connection);
         referencesQuery.query(d -> {
            Message message = loadedMessages.get(d.messageID);
            if (message != null) {
               try {
                  journalLoader.handleJDBCAdd(message, d);
               } catch (Exception e) {
                  // TODO-IMPORTANT Critical Error?
                  logger.warn(e.getMessage(), e);
               }
            }
         });

      }
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
   public void deleteGrouping(Transaction tx, GroupBinding groupBinding) throws Exception {
      journalDelegate.deleteGrouping(tx, groupBinding);
   }

   @Override
   public void updateQueueBinding(Transaction tx, Binding binding, AddressInfo addressInfo) throws Exception {
      journalDelegate.updateQueueBinding(tx, binding, addressInfo);
   }

   @Override
   public void addQueueBinding(Transaction tx, Binding binding, AddressInfo addressInfo) throws Exception {
      RoutingType routingType;
      if (binding instanceof LocalQueueBinding) {
         routingType = ((LocalQueueBinding) binding).getQueue().getRoutingType();
      } else {
         routingType = null;
      }
      dataManager.storeQueue(tx.getStorageTx(), addressInfo.getId(), binding.getID(), String.valueOf(binding.getUniqueName()), binding.getFilter() != null ? String.valueOf(binding.getFilter().getFilterString()) : null, routingType, getContext());
   }

   @Override
   public void deleteQueueBinding(Transaction tx, long queueBindingID) throws Exception {
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
   public void addAddressBinding(Transaction tx, AddressInfo addressInfo) throws Exception {
      addressInfo.setId(generateID());
      dataManager.storeAddressInfo(tx.getStorageTx(), addressInfo, getContext());
   }

   @Override
   public void deleteAddressBinding(Transaction tx, long addressBindingID) throws Exception {
      journalDelegate.deleteAddressBinding(tx, addressBindingID);
   }

   @Override
   public long storePageCounterInc(Transaction tx, long queueID, int value, long persistentSize) throws Exception {
      return journalDelegate.storePageCounterInc(tx, queueID, value, persistentSize);
   }

   @Override
   public long storePageCounterInc(long queueID, int value, long persistentSize) throws Exception {
      return journalDelegate.storePageCounterInc(queueID, value, persistentSize);
   }

   @Override
   public long storePageCounter(Transaction tx, long queueID, long value, long persistentSize) throws Exception {
      return journalDelegate.storePageCounter(tx, queueID, value, persistentSize);
   }

   @Override
   public long storePendingCounter(long queueID, long pageID) throws Exception {
      return journalDelegate.storePendingCounter(queueID, pageID);
   }

   @Override
   public void deleteIncrementRecord(Transaction tx, long recordID) throws Exception {
      journalDelegate.deleteIncrementRecord(tx, recordID);
   }

   @Override
   public void deletePageCounter(Transaction tx, long recordID) throws Exception {
      journalDelegate.deletePageCounter(tx, recordID);
   }

   @Override
   public void deletePendingPageCounter(Transaction tx, long recordID) throws Exception {
      journalDelegate.deletePendingPageCounter(tx, recordID);
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
