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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

public class SessionExpireMessage extends PacketImpl {


   private long consumerID;

   private long messageID;


   public SessionExpireMessage(final long consumerID, final long messageID) {
      super(SESS_EXPIRED);

      this.consumerID = consumerID;

      this.messageID = messageID;
   }

   public SessionExpireMessage() {
      super(SESS_EXPIRED);
   }

   public long getConsumerID() {
      return consumerID;
   }

   public long getMessageID() {
      return messageID;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeLong(consumerID);

      buffer.writeLong(messageID);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      consumerID = buffer.readLong();

      messageID = buffer.readLong();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int) (consumerID ^ (consumerID >>> 32));
      result = prime * result + (int) (messageID ^ (messageID >>> 32));
      return result;
   }

   @Override
   protected String getPacketString() {
      StringBuffer buff = new StringBuffer(super.getPacketString());
      buff.append(", consumerID=" + consumerID);
      buff.append(", messageID=" + messageID);
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionExpireMessage))
         return false;
      SessionExpireMessage other = (SessionExpireMessage) obj;
      if (consumerID != other.consumerID)
         return false;
      if (messageID != other.messageID)
         return false;
      return true;
   }
}
