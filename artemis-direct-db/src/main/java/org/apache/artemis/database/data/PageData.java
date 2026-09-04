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

package org.apache.artemis.database.data;

import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.artemis.database.worker.DataWorker;

public class PageData extends DBData {

   public final long addressID;
   public final long pageID;
   public final long pageNR;
   public final long messageID;
   public final Supplier<ActiveMQBuffer> messageBufferSupplier;
   public final Long txID;

   public PageData(long addressID, long pageID, long pageNR, long messageID, Supplier<ActiveMQBuffer> messageBufferSupplier, Long txID, IOCompletion context) {
      super(context);
      this.addressID = addressID;
      this.pageID = pageID;
      this.pageNR = pageNR;
      this.messageID = messageID;
      this.messageBufferSupplier = messageBufferSupplier;
      this.txID = txID;
   }

   public PageData(long addressID, long pageID, long pageNR, long messageID, Supplier<ActiveMQBuffer> messageBufferSupplier, Long txID) {
      this(addressID, pageID, pageNR, messageID, messageBufferSupplier, txID, null);
   }

   @Override
   public void perform(DataWorker worker) {
      worker.insertPageStatement.addElement(this, context);
   }

   @Override
   public String toString() {
      return "PageData{" + "addressID=" + addressID + ", pageID=" + pageID + ", pageNR=" + pageNR + ", messageID=" + messageID + ", txID=" + txID + '}';
   }
}
