package org.apache.pinot.integration.tests.logicaltable;

import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public class LogicalTableWithGrpcIntegrationTest extends BaseLogicalTableIntegrationTest {
  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.BROKER_REQUEST_HANDLER_TYPE, "grpc");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_GRPC_SERVER, true);
  }

  @Override
  protected void startServer()
      throws Exception {
    // Start two servers
    startServers(2);
  }

  protected List<String> getOfflineTableNames() {
    return List.of("physicalTable_0", "physicalTable_1", "physicalTable_2", "physicalTable_3", "physicalTable_4",
        "physicalTable_5", "physicalTable_6", "physicalTable_7", "physicalTable_8", "physicalTable_9",
        "physicalTable_10", "physicalTable_11");
  }

}
