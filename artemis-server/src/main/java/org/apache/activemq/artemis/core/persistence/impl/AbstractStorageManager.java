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

package org.apache.activemq.artemis.core.persistence.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.OperationConsistencyLevel;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.apache.activemq.artemis.utils.critical.CriticalComponentImpl;

public abstract class AbstractStorageManager extends CriticalComponentImpl implements StorageManager {

   protected final ExecutorFactory ioExecutorFactory;

   protected final ScheduledExecutorService scheduledExecutorService;

   protected final ExecutorFactory executorFactory;

   protected final Executor executor;

   @Override
   public void clearContext() {
      OperationContextImpl.clearContext();
   }

   @Override
   public OperationContext getContext() {
      return OperationContextImpl.getContext(executorFactory);
   }

   @Override
   public void setContext(final OperationContext context) {
      OperationContextImpl.setContext(context);
   }

   @Override
   public OperationContext newSingleThreadContext() {
      return newContext(executor);
   }

   @Override
   public OperationContext newContext(final Executor executor1) {
      return new OperationContextImpl(executor1);
   }

   @Override
   public void afterCompleteOperations(final IOCallback run) {
      getContext().executeOnCompletion(run);
   }

   @Override
   public void afterCompleteOperations(final IOCallback run, OperationConsistencyLevel consistencyLevel) {
      getContext().executeOnCompletion(run, consistencyLevel);
   }

   @Override
   public void afterStoreOperations(IOCallback run) {
      getContext().executeOnCompletion(run, OperationConsistencyLevel.STORAGE);
   }

   public AbstractStorageManager(CriticalAnalyzer analyzer, int numberOfPaths, ExecutorFactory executorFactory, ScheduledExecutorService scheduledExecutorService, ExecutorFactory ioExecutorFactory) {
      super(analyzer, numberOfPaths);
      this.ioExecutorFactory = ioExecutorFactory;
      this.scheduledExecutorService = scheduledExecutorService;
      this.executorFactory = executorFactory;

      executor = executorFactory.getExecutor();
   }
}
