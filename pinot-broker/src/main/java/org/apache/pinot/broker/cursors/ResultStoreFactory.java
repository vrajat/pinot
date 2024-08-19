package org.apache.pinot.broker.cursors;

import java.util.Map;
import org.apache.pinot.broker.cursors.file.FsResultStore;
import org.apache.pinot.broker.cursors.memory.MemoryResultStore;
import org.apache.pinot.common.auth.AuthConfig;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResultStoreFactory {
  private ResultStoreFactory() {

  }

  static final String RESULT_STORE_CLASS_KEY_SUFFIX = ".class";
  private static final String PROTOCOL_KEY = "protocol";
  private static final String DEFAULT_PROTOCOL = "memory";
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultStoreFactory.class);

  public static ResultStore create(PinotConfiguration config)
      throws Exception {
    String protocol = config.getProperty(PROTOCOL_KEY, DEFAULT_PROTOCOL);
    ResultStore resultStore;
    if (protocol.equals("memory")) {
      resultStore = new MemoryResultStore();
    } else {
      resultStore = new FsResultStore();
    }

    AuthConfig authConfig = AuthProviderUtils.extractAuthConfig(config, CommonConstants.KEY_OF_AUTH);

    PinotConfiguration subConfig = config.subset(protocol);
    AuthConfig subAuthConfig = AuthProviderUtils.extractAuthConfig(subConfig, CommonConstants.KEY_OF_AUTH);

    Map<String, Object> subConfigMap = config.subset(protocol).toMap();
    if (subAuthConfig.getProperties().isEmpty() && !authConfig.getProperties().isEmpty()) {
      authConfig.getProperties().forEach((key, value) -> subConfigMap.put(CommonConstants.KEY_OF_AUTH + "." + key,
          value));
    }

    // Add temp dir to the sub config so that the result store can use it if required.
    // Effectively this is moving the config from pinot.broker.pagination.temp.dir to
    // pinot.broker.pagination.<protocol>.temp.dir

    if (config.containsKey(CommonConstants.CursorConfigs.TEMP_DIR)) {
      subConfigMap.put(CommonConstants.CursorConfigs.TEMP_DIR,
          config.getProperty(CommonConstants.CursorConfigs.TEMP_DIR));
    }
    resultStore.init(new PinotConfiguration(subConfigMap));
    return resultStore;
  }
}
