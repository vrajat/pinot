package org.apache.pinot.broker.cursors.memory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.broker.cursors.QueryStore;
import org.apache.pinot.broker.cursors.ResultMetadata;
import org.apache.pinot.broker.cursors.ResultStore;
import org.apache.pinot.spi.env.PinotConfiguration;


public class MemoryResultStore implements ResultStore {
  private final ConcurrentHashMap<String, MemoryQueryStore> _queryStores;

  public MemoryResultStore() {
    _queryStores = new ConcurrentHashMap<>();
  }

  @Override
  public void init(PinotConfiguration config) {
  }

  @Override
  public QueryStore initQueryStore(ResultMetadata metadata) {
    MemoryQueryStore queryStore = new MemoryQueryStore(metadata);
    _queryStores.put(metadata.getRequestId(), queryStore);
    return queryStore;
  }

  @Override
  public QueryStore getQueryStore(String requestId) {
    return _queryStores.get(requestId);
  }

  @Override
  public Collection<? extends QueryStore> getAllQueryStores() {
    return _queryStores.values();
  }

  @Override
  public QueryStore deleteQueryStore(String requestId) {
    return _queryStores.remove(requestId);
  }
}
