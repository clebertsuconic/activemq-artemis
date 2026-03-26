/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.broker;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.utils.DataConstants;

import static org.apache.activemq.artemis.core.persistence.PersisterIDs.AMQPLargeMessagePersisterV2_ID;

public class AMQPLargeMessagePersisterV2 extends AMQPLargeMessagePersister {

   public static final byte ID = AMQPLargeMessagePersisterV2_ID;

   public static AMQPLargeMessagePersisterV2 theInstance;

   public static AMQPLargeMessagePersisterV2 getInstance() {
      if (theInstance == null) {
         theInstance = new AMQPLargeMessagePersisterV2();
      }
      return theInstance;
   }

   @Override
   public byte getID() {
      return ID;
   }

   public AMQPLargeMessagePersisterV2() {
      super();
   }


   protected static final int PERSISTER_SIZE = DataConstants.SIZE_INT + // memory estimate
                                             DataConstants.SIZE_BYTE + // message priority
                                             DataConstants.SIZE_BOOLEAN; // durable

   @Override
   public int getEncodeSize(Message record) {
      return super.getEncodeSize(record) +
         DataConstants.SIZE_INT + // size delimiter for future use to keep compatibility better
         PERSISTER_SIZE;
   }

   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);

      AMQPLargeMessage msgEncode = (AMQPLargeMessage) record;
      writeSizeDelimiter(buffer);
      buffer.writeInt(msgEncode.getMemoryEstimate());
      buffer.writeByte(msgEncode.getPriority());
      buffer.writeBoolean(msgEncode.isDurable());
   }

   protected void writeSizeDelimiter(ActiveMQBuffer buffer) {
      buffer.writeInt(PERSISTER_SIZE); // how many bytes this persister is using
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message record, CoreMessageObjectPools pools) {
      AMQPLargeMessage message = (AMQPLargeMessage) super.decode(buffer, record, pools);


      int sizePersister = buffer.readInt();
      int lastPosition = buffer.readerIndex() + sizePersister;

      {
         message.setMemoryEstimate(buffer.readInt());
         message.setPriority(buffer.readByte());
         message.reloadSetDurable(buffer.readBoolean());

         assert buffer.readerIndex() <= lastPosition;
      }

      // if a future version of this persister wrote more bytes than what we expected now, this will make sure we skip them.
      buffer.readerIndex(lastPosition);

      return message;
   }

}
