package org.apache.pinot.broker.cursors.memory;

import com.google.auto.service.AutoService;
import org.apache.pinot.spi.cursors.ResultStore;
import org.apache.pinot.spi.cursors.ResultStoreFactory;
import org.apache.pinot.spi.env.PinotConfiguration;


@AutoService(ResultStoreFactory.class)
public class MemoryResultStoreFactory implements ResultStoreFactory {
  private static final String TYPE = "memory";
  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public ResultStore create(PinotConfiguration configuration) {
    return new MemoryResultStore();
  }
}
