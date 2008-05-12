/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.core.message.impl;

import static org.jboss.messaging.util.DataConstants.BOOLEAN;
import static org.jboss.messaging.util.DataConstants.BYTE;
import static org.jboss.messaging.util.DataConstants.BYTES;
import static org.jboss.messaging.util.DataConstants.CHAR;
import static org.jboss.messaging.util.DataConstants.DOUBLE;
import static org.jboss.messaging.util.DataConstants.FLOAT;
import static org.jboss.messaging.util.DataConstants.INT;
import static org.jboss.messaging.util.DataConstants.LONG;
import static org.jboss.messaging.util.DataConstants.NOT_NULL;
import static org.jboss.messaging.util.DataConstants.NULL;
import static org.jboss.messaging.util.DataConstants.SHORT;
import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;
import static org.jboss.messaging.util.DataConstants.SIZE_BYTE;
import static org.jboss.messaging.util.DataConstants.SIZE_CHAR;
import static org.jboss.messaging.util.DataConstants.SIZE_DOUBLE;
import static org.jboss.messaging.util.DataConstants.SIZE_FLOAT;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;
import static org.jboss.messaging.util.DataConstants.SIZE_SHORT;
import static org.jboss.messaging.util.DataConstants.STRING;

import java.util.Set;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.impl.mina.BufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TypedProperties;

/**
 * A concrete implementation of a message
 * 
 * All messages handled by JBM core are of this type
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2740 $</tt>
 * 
 * For normal message transportation serialization is not used
 * 
 * $Id: MessageSupport.java 2740 2007-05-30 11:36:28Z timfox $
 */
public abstract class MessageImpl implements Message
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(MessageImpl.class);

   // Attributes ----------------------------------------------------

   private SimpleString destination;
   
   private int type;
   
   protected boolean durable;

   /** GMT milliseconds at which this message expires. 0 means never expires * */
   private long expiration;

   private long timestamp;

   private TypedProperties properties;
   
   private byte priority;


   private MessagingBuffer body;
   
   // Constructors --------------------------------------------------

   protected MessageImpl()
   {
      this.properties = new TypedProperties();
   }
   
   protected MessageImpl(final int type, final boolean durable, final long expiration,
                      final long timestamp, final byte priority)
   {
      this();
      this.type = type;
      this.durable = durable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;            
      this.body = new BufferWrapper(1024);
   }
   
   /*
    * Copy constructor
    */
   protected MessageImpl(final MessageImpl other)
   {
      this.destination = other.destination;
      this.type = other.type;
      this.durable = other.durable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.priority = other.priority;
      this.properties = new TypedProperties(other.properties);
      this.body = other.body;
   }
   
   // Message implementation ----------------------------------------

   public void encode(MessagingBuffer buff)
   {
//      buff.putSimpleString(destination);
//      buff.putInt(type);
//      buff.putBoolean(durable);
//      buff.putLong(expiration);
//      buff.putLong(timestamp);
//      buff.putByte(priority);
//      properties.encode(buff);
//      buff.putInt(body.limit());
//      buff.putBytes(body.array(), 0, body.limit());

   
      buff.putSimpleString(destination);
      buff.putInt(type);
      buff.putBoolean(durable);
      buff.putLong(expiration);
      buff.putLong(timestamp);
      buff.putByte(priority);
      properties.encode(buff);
      buff.putInt(body.limit());
      buff.putBytes(body.array(), 0, body.limit());
   
   }
   
   public int encodeSize()
   {
//      return /* Destination */ SimpleString.sizeofString(destination) + 
//             /* Type */ SIZE_INT + 
//             /* Durable */ SIZE_BOOLEAN + 
//             /* Expiration */ SIZE_LONG + 
//             /* Timestamp */ SIZE_LONG +
//             /* Priority */  SIZE_BYTE + 
//             /* PropertySize and Properties */ properties.encodeSize() + 
//             /* BodySize and Body */ SIZE_INT + body.limit();
      return /* Destination */ SimpleString.sizeofString(destination) + 
      /* Type */ SIZE_INT + 
      /* Durable */ SIZE_BOOLEAN + 
      /* Expiration */ SIZE_LONG + 
      /* Timestamp */ SIZE_LONG + 
      /* Priority */ SIZE_BYTE + 
      /* PropertySize and Properties */ properties.encodeSize() + 
      /* BodySize and Body */ SIZE_INT + body.limit();
      
   }
   
   public void decode(final MessagingBuffer buffer)
   {
      destination = buffer.getSimpleString();
      type = buffer.getInt();
      durable = buffer.getBoolean();
      expiration = buffer.getLong();
      timestamp = buffer.getLong();
      priority = buffer.getByte();
      
      properties.decode(buffer);
      int len = buffer.getInt();
      
      //TODO - this can be optimised
      byte[] bytes = new byte[len];
      buffer.getBytes(bytes);
      body = new BufferWrapper(1024);
      body.putBytes(bytes);      
   }
   
   public SimpleString getDestination()
   {
      return destination;
   }
   
   public void setDestination(SimpleString destination)
   {
      this.destination = destination;
   }
   
   public int getType()
   {
      return type;
   }

   public boolean isDurable()
   {
      return durable;
   }
   
   public void setDurable(final boolean durable)
   {
      this.durable = durable;
   }

   public long getExpiration()
   {
      return expiration;
   }

   public void setExpiration(final long expiration)
   {
      this.expiration = expiration;
   }

   public long getTimestamp()
   {
      return timestamp;
   }
   
   public void setTimestamp(final long timestamp)
   {
      this.timestamp = timestamp;
   }

 
   public byte getPriority()
   {
      return priority;
   }

   public void setPriority(final byte priority)
   {
      this.priority = priority;
   }
     

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }
      
      return System.currentTimeMillis() - expiration >= 0;
   }
   
   // Properties 
   // ---------------------------------------------------------------------------------------
   
   public void putBooleanProperty(final SimpleString key, final boolean value)
   {
      properties.putBooleanProperty(key, value);
   }
            
   public void putByteProperty(final SimpleString key, final byte value)
   {
      properties.putByteProperty(key, value);
   }
   
   public void putBytesProperty(final SimpleString key, final byte[] value)
   {
      properties.putBytesProperty(key, value);
   }
   
   public void putShortProperty(final SimpleString key, final short value)
   {
      properties.putShortProperty(key, value);
   }
   
   public void putIntProperty(final SimpleString key, final int value)
   {
      properties.putIntProperty(key, value);
   }
   
   public void putLongProperty(final SimpleString key, final long value)
   {
      properties.putLongProperty(key, value);
   }
   
   public void putFloatProperty(final SimpleString key, final float value)
   {
      properties.putFloatProperty(key, value);
   }
   
   public void putDoubleProperty(final SimpleString key, final double value)
   {
      properties.putDoubleProperty(key, value);
   }
   
   public void putStringProperty(final SimpleString key, final SimpleString value)
   {
      properties.putStringProperty(key, value);
   }
   
   public Object getProperty(final SimpleString key)
   {
      return properties.getProperty(key);
   }  
   
   public Object removeProperty(final SimpleString key)
   {
      return properties.removeProperty(key);
   }
   
   public boolean containsProperty(final SimpleString key)
   {
      return properties.containsProperty(key);
   }
   
   public Set<SimpleString> getPropertyNames()
   {
      return properties.getPropertyNames();
   }
   
   // Body
   // -------------------------------------------------------------------------------------
   
   public MessagingBuffer getBody()
   {
      return body;
   }
   
   public void setBody(final MessagingBuffer body)
   {
      this.body = body;
   }
      
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------  
}
