package org.apache.pinot.spi.cursors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;


@ThreadSafe
public class ResultStoreService {
  private static volatile ResultStoreService _instance = fromServiceLoader();

  private final Set<ResultStoreFactory> _allResultStoreFactories;
  private final Map<String, ResultStoreFactory> _resultStoreFactoryByType;

  private ResultStoreService(Set<ResultStoreFactory> storeSet) {
    _allResultStoreFactories = storeSet;
    _resultStoreFactoryByType = new HashMap<>();

    for (ResultStoreFactory resultStoreFactory : storeSet) {
      _resultStoreFactoryByType.put(resultStoreFactory.getType(), resultStoreFactory);
    }
  }

  public static ResultStoreService getInstance() {
    return _instance;
  }

  public static void setInstance(ResultStoreService service) {
    _instance = service;
  }

  public static ResultStoreService fromServiceLoader() {
    Set<ResultStoreFactory> storeSet = new HashSet<>();
    for (ResultStoreFactory resultStoreFactory : ServiceLoader.load(ResultStoreFactory.class)) {
      storeSet.add(resultStoreFactory);
    }

    return new ResultStoreService(storeSet);
  }

  public Set<ResultStoreFactory> getAllResultStoreFactories() {
    return _allResultStoreFactories;
  }

  public Map<String, ResultStoreFactory> getResultStoreFactoriesByType() {
    return _resultStoreFactoryByType;
  }

  public ResultStoreFactory getResultStoreFactory(String type) {
    ResultStoreFactory resultStoreFactory = _resultStoreFactoryByType.get(type);

    if (resultStoreFactory == null) {
      throw new IllegalArgumentException("Unknown ResultStoreFactory type: " + type);
    }

    return resultStoreFactory;
  }
}
