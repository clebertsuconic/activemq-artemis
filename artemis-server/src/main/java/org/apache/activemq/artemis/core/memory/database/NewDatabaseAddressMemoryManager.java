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

package org.apache.activemq.artemis.core.memory.database;

import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.memory.AddressMemoryManager;
import org.apache.activemq.artemis.core.memory.GlobalMemoryManager;
import org.apache.activemq.artemis.core.memory.QueueMemoryManager;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;

public class NewDatabaseAddressMemoryManager implements AddressMemoryManager {

   @Override
   public long getAddressElements() {
      return 0;
   }

   final NewDatabaseGlobalMemoryManager globalMemoryManager;
   final SimpleString name;

   public NewDatabaseAddressMemoryManager(SimpleString name, long addressID, NewDatabaseGlobalMemoryManager globalMemoryManager) {
      this.globalMemoryManager = globalMemoryManager;
      this.name = name;

   }

   @Override
   public SimpleString getName() {
      return name;
   }

   @Override
   public GlobalMemoryManager getGlobalMemoryManager() {
      return globalMemoryManager;
   }

   @Override
   public void addSize(int size, boolean sizeOnly, boolean affectGlobal) {

   }

   @Override
   public long getAddressSize() {
      return 0;
   }

   @Override
   public QueueMemoryManager getQueueMemoryManager(SimpleString queue,
                                                   long queueID,
                                                   Filter filter,
                                                   boolean durable) throws Exception {
      // TODO-NOW: implement this
      return new NewDatabaseQueueMemoryManager();
   }

   @Override
   public QueueMemoryManager getQueueMemoryManager(long queueID) {
      return null;
   }

   @Override
   public boolean checkMemory(Runnable runnable, Consumer<AtomicRunnable> blockedCallback) {
      runnable.run();
      return true;
   }

   @Override
   public boolean checkMemory(boolean runOnFailure,
                              Runnable runnable,
                              Runnable runWhenBlocking,
                              Consumer<AtomicRunnable> blockedCallback) {
      runnable.run();
      return true;
   }

   @Override
   public AddressFullMessagePolicy getAddressFullMessagePolicy() {
      return AddressFullMessagePolicy.PAGE;
   }

   @Override
   public boolean isStorePaging() {
      return false;
   }

   @Override
   public ArtemisExecutor getExecutor() {
      return null;
   }

   @Override
   public long getMaxSize() {
      return 0;
   }

   @Override
   public int getMaxReadMessages() {
      return 0;
   }

   @Override
   public int getMaxReadBytes() {
      return 0;
   }

   @Override
   public int getPrefetchMessages() {
      return 0;
   }

   @Override
   public int getPrefetchBytes() {
      return 0;
   }

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
}
