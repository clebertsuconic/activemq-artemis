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

package org.apache.activemq.artemis.core.persistence.impl.parallelDB.dbdata;

import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.worker.DataWorker;

public class DeleteReferenceData extends DBData {
   public long messageID;
   public long queueID;

   public DeleteReferenceData(long messageID, long queueID, IOCompletion context) {
      super(context);
      this.messageID = messageID;
      this.queueID = queueID;
      context.storeLineUp();
   }

   public void store(DataWorker worker) {
      worker.deleteReferenceStatement.addData(this, context);
   }

   @Override
   public String toString() {
      return "MessageReferenceTask{" + "messageID=" + messageID + ", queueID=" + queueID + '}';
   }
}