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
package org.apache.activemq.artemis.core.paging.impl;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.impl.DatabaseCursorProvider;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.server.StorageMessageReader;
import org.apache.activemq.artemis.core.server.impl.DatabaseStorageMessageReader;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.PageFullMessagePolicy;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;

public class DatabasePagingStore implements PagingStore {

   private final SimpleString address;

   private final DatabaseCursorProvider cursorProvider = new DatabaseCursorProvider();

   public DatabasePagingStore(SimpleString address) {
      this.address = address;
   }

   @Override
   public PageCursorProvider getCursorProvider() {
      return cursorProvider;
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public long getNumberOfPages() {
      return 0;
   }

   @Override
   public long getCurrentWritingPage() {
      return 0;
   }

   @Override
   public SimpleString getStoreName() {
      return address;
   }

   @Override
   public File getFolder() {
      return null;
   }

   @Override
   public AddressFullMessagePolicy getAddressFullMessagePolicy() {
      return AddressFullMessagePolicy.PAGE;
   }

   @Override
   public PageFullMessagePolicy getPageFullMessagePolicy() {
      return null;
   }

   @Override
   public Long getPageLimitMessages() {
      return null;
   }

   @Override
   public Long getPageLimitBytes() {
      return null;
   }

   @Override
   public void pageFull(PageSubscription subscription) {
   }

   @Override
   public boolean isPageFull() {
      return false;
   }

   @Override
   public void checkPageLimit(long numberOfMessages) {
   }

   @Override
   public long getFirstPage() {
      return 0;
   }

   @Override
   public int getPageSizeBytes() {
      return 0;
   }

   @Override
   public long getAddressSize() {
      return 0;
   }

   @Override
   public long getAddressElements() {
      return 0;
   }

   @Override
   public long getMaxSize() {
      return -1;
   }

   @Override
   public int getMaxPageReadBytes() {
      return -1;
   }

   @Override
   public int getMaxPageReadMessages() {
      return -1;
   }

   @Override
   public int getPrefetchPageBytes() {
      return -1;
   }

   @Override
   public int getPrefetchPageMessages() {
      return -1;
   }

   @Override
   public void applySetting(AddressSettings addressSettings) {
   }

   @Override
   public boolean isPaging() {
      return false;
   }

   @Override
   public boolean isStorePaging() {
      return false;
   }

   @Override
   public void ioSync() throws Exception {
   }

   @Override
   public boolean page(Message message, Transaction tx, RouteContextList listCtx) throws Exception {
      return false;
   }

   @Override
   public int page(Message message, Transaction tx, RouteContextList listCtx, Function<Message, Message> pageDecorator, boolean useFlowControl) throws Exception {
      return 0;
   }

   @Override
   public Page usePage(long page) {
      return null;
   }

   @Override
   public Page usePage(long page, boolean create) {
      return null;
   }

   @Override
   public Page usePage(long page, boolean createEntry, boolean createFile) {
      return null;
   }

   @Override
   public Page newPageObject(long page) throws Exception {
      return null;
   }

   @Override
   public boolean checkPageFileExists(long page) throws Exception {
      return false;
   }

   @Override
   public PagingManager getPagingManager() {
      return null;
   }

   @Override
   public void processReload() throws Exception {
   }

   @Override
   public Page depage() throws Exception {
      return null;
   }

   @Override
   public Page removePage(int pageId) {
      return null;
   }

   @Override
   public void forceAnotherPage(boolean useExecutor) throws Exception {
   }

   @Override
   public Page getCurrentPage() {
      return null;
   }

   @Override
   public void counterSnapshot() {
   }

   @Override
   public boolean startPaging() throws Exception {
      return false;
   }

   @Override
   public void stopPaging() throws Exception {
   }

   @Override
   public void addSize(int size, boolean sizeOnly, boolean affectGlobal) {
   }

   @Override
   public boolean checkMemory(Runnable runnable, Consumer<AtomicRunnable> blockedCallback) {
      runnable.run();
      return true;
   }

   @Override
   public boolean checkMemory(boolean runOnFailure, Runnable runnable, Runnable runWhenBlocking, Consumer<AtomicRunnable> blockedCallback) {
      runnable.run();
      return true;
   }

   @Override
   public boolean isFull() {
      return false;
   }

   @Override
   public boolean isRejectingMessages() {
      return false;
   }

   @Override
   public boolean checkReleasedMemory() {
      return true;
   }

   @Override
   public void writeLock() {
   }

   @Override
   public boolean writeLock(long timeout) {
      return true;
   }

   @Override
   public void writeUnlock() {
   }

   @Override
   public void readLock() {
   }

   @Override
   public boolean readLock(long timeout) {
      return true;
   }

   @Override
   public void readUnlock() {
   }

   @Override
   public void flushExecutors() {
   }

   @Override
   public void execute(Runnable runnable) {
      runnable.run();
   }

   @Override
   public ArtemisExecutor getExecutor() {
      return null;
   }

   @Override
   public Collection<Integer> getCurrentIds() throws Exception {
      return Collections.emptyList();
   }

   @Override
   public void sendPages(ReplicationManager replicator, Collection<Integer> pageIds) throws Exception {
   }

   @Override
   public void disableCleanup() {
   }

   @Override
   public void enableCleanup() {
   }

   @Override
   public void destroy() throws Exception {
   }

   @Override
   public int getAddressLimitPercent() {
      return 0;
   }

   @Override
   public void block() {
   }

   @Override
   public void unblock() {
   }

   @Override
   public boolean isBlockedViaManagement() {
      return false;
   }

   // ActiveMQComponent methods

   @Override
   public boolean isStarted() {
      return true;
   }

   @Override
   public void start() throws Exception {
   }

   @Override
   public void stop() throws Exception {
   }

   // RefCountMessageListener methods

   @Override
   public void durableUp(Message message, int durableCount) {
   }

   @Override
   public void durableDown(Message message, int durableCount) {
   }

   @Override
   public void refUp(Message message, int nonDurableCount) {
   }

   @Override
   public void refDown(Message message, int nonDurableCount) {
   }

   @Override
   public StorageMessageReader createStorageMessageReader(QueueImpl queue) {
      return new DatabaseStorageMessageReader(queue);
   }
}
