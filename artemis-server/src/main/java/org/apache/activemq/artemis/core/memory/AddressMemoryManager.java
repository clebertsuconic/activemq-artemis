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

package org.apache.activemq.artemis.core.memory;

import java.util.function.Consumer;

import org.apache.activemq.artemis.api.core.RefCountMessageListener;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;

public interface AddressMemoryManager extends RefCountMessageListener {

   GlobalMemoryManager getGlobalMemoryManager();

   SimpleString getName();

   void addSize(int size, boolean sizeOnly, boolean affectGlobal);

   long getAddressSize();
   long getAddressElements();

   QueueMemoryManager getQueueMemoryManager(SimpleString queue, long queueID, Filter filter, boolean durable) throws Exception;

   QueueMemoryManager getQueueMemoryManager(long queueID);

   boolean checkMemory(Runnable runnable, Consumer<AtomicRunnable> blockedCallback);

   boolean checkMemory(boolean runOnFailure, Runnable runnable, Runnable runWhenBlocking, Consumer<AtomicRunnable> blockedCallback);

   default void disableCleanup() {
   }

   default void enableCleanup() {
   }

   default void scheduleCleanup() {
   }

   default boolean isRejectingMessages() {
      return false;
   }

   AddressFullMessagePolicy getAddressFullMessagePolicy();

   boolean isStorePaging();

   ArtemisExecutor getExecutor();

   default void execute(Runnable runnable) {
      runnable.run();
   }

   long getMaxSize();

   int getMaxReadMessages();

   int getMaxReadBytes();

   int getPrefetchMessages();

   int getPrefetchBytes();

}
