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
package org.apache.activemq.artemis.core.paging.cursor.impl;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasePagingCounter implements PageSubscriptionCounter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private volatile  boolean rebuilding = false;

   @Override
   public void markRebuilding() {
      rebuilding = true;
   }

   @Override
   public void finishRebuild() {

      rebuilding = false;
   }

   @Override
   public boolean isRebuilding() {
      return rebuilding;
   }

}
