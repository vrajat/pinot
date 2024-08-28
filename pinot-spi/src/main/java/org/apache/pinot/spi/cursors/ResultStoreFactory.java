package org.apache.pinot.spi.cursors;

import org.apache.pinot.spi.env.PinotConfiguration;


public interface ResultStoreFactory {
  String getType();
  ResultStore create(PinotConfiguration configuration);
}
