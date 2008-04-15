/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionCreateBrowserResponseMessageCodec extends
      AbstractPacketCodec<SessionCreateBrowserResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserResponseMessageCodec()
   {
      super(SESS_CREATEBROWSER_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   protected int getBodyLength(final SessionCreateBrowserResponseMessage packet) throws Exception
   {   	
      return LONG_LENGTH;
   }
   
   @Override
   protected void encodeBody(final SessionCreateBrowserResponseMessage response,
         final RemotingBuffer out) throws Exception
   {
      long browserID = response.getBrowserTargetID();

      out.putLong(browserID);
   }

   @Override
   protected SessionCreateBrowserResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      long browserID = in.getLong();

      return new SessionCreateBrowserResponseMessage(browserID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
