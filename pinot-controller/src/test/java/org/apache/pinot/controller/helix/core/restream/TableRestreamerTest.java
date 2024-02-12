package org.apache.pinot.controller.helix.core.restream;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TableRestreamerTest extends ControllerTest {
  private static Logger logger = LoggerFactory.getLogger(TableRestreamerTest.class);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final int NUM_REPLICAS = 1;
  private static final int NUM_INSTANCES = 2;
  private static final List<String> tags = ImmutableList.of("default", "restream");
  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_INSTANCES, false);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_INSTANCES, false);

    for (String tag : tags) {
      createBrokerTenant(tag, NUM_INSTANCES/tags.size());
      createServerTenant(tag, 0, NUM_INSTANCES/tags.size());
    }
  }

  @Test
  void testTenantsExist() throws IOException {
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTenantGet()));
    Assert.assertEquals(response.get("SERVER_TENANTS").size(), tags.size());

    response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTenantGet()));
    Assert.assertEquals(response.get("BROKER_TENANTS").size(), tags.size());
  }

  @AfterClass
  public void tearDown() {
    try {
      for (String tag : tags) {
        deleteBrokerTenant(tag);
        sendDeleteRequest(_controllerRequestURLBuilder.forServerTenantDelete(tag));
      }
    } catch (IOException exc) {
      logger.error("Failed to delete tag.", exc);
    }
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
