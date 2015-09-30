package org.apache.activemq.broker;

import org.apache.activemq.network.NetworkConnector;

import java.net.URI;

/**
 */
public class FakeNetworkConnector extends NetworkConnector {
   public FakeNetworkConnector(URI uri) {
      this.setLocalUri(uri);
   }
}
