package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.restlet.resources.ResultResponse;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.cursors.ResultStoreCleaner;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class CursorIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(CursorIntegrationTest.class);
  protected static final String TENANT_NAME = "TestTenant";
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;
  private static final String TEST_QUERY_ONE =
      "SELECT SUM(CAST(CAST(ArrTime AS varchar) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = "
          + "'DL'";
  private static final String TEST_QUERY_TWO =
      "SELECT CAST(CAST(ArrTime AS varchar) AS LONG) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL' "
          + "ORDER BY ArrTime DESC";
  private static final String TEST_QUERY_THREE =
      "SELECT ArrDelay, CarrierDelay, (ArrDelay - CarrierDelay) AS diff FROM mytable WHERE ArrDelay > CarrierDelay "
          + "ORDER BY diff, ArrDelay, CarrierDelay LIMIT 100000";
  private static final String EMPTY_RESULT_QUERY =
      "SELECT SUM(CAST(CAST(ArrTime AS varchar) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND 1 != 1";

  private static int _resultSize;

  @Override
  protected String getBrokerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected String getServerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    properties.put(CommonConstants.CursorConfigs.RESULT_STORE_CLEANER_FREQUENCY_PERIOD, "5m");
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_INSTANCE_TAGS,
        TagNameUtils.getBrokerTagForTenant(TENANT_NAME));
    configuration.setProperty(CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_CURSOR + ".protocol",
        "memory");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    List<File> avroFiles = getAllAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_REALTIME_SEGMENTS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    getControllerRequestClient().addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir,
        _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(100_000L);
  }

  protected String getBrokerPagingQueryApiUrl(String brokerBaseApiUrl, int numRows) {
    return brokerBaseApiUrl + "/stp/query/sql" + (numRows > 0 ? "?numRows=" + numRows : "");
  }

  protected String getBrokerGetAllQueryStoresApiUrl(String brokerBaseApiUrl) {
    return brokerBaseApiUrl + "/stp/resultStore";
  }

  protected String getBrokerDeleteQueryStoresApiUrl(String brokerBaseApiUrl, String requestId) {
    return brokerBaseApiUrl + "/stp/resultStore/" + requestId;
  }

  protected String getBrokerQueryApiUrl(String brokerBaseApiUrl) {
    return brokerBaseApiUrl + "/query/sql";
  }

  protected Map<String, String> getCursorQueryProperties(int numRows) {
    return Map.of(CommonConstants.Broker.Request.QUERY_OPTIONS,
        String.format("%s=%s;%s=%s", CommonConstants.Broker.Request.QueryOptionKey.GET_CURSOR, "true",
            CommonConstants.Broker.Request.QueryOptionKey.GET_CURSOR_NUM_ROWS, numRows));
  }

  protected Map<String, String> getCursorOffset(String requestId, String nextOffsetParams) {
    Map<String, String> params = Splitter.on('&').omitEmptyStrings().trimResults().withKeyValueSeparator('=').split(nextOffsetParams);
    Map<String, String> options = new java.util.HashMap<>(Map.of(CommonConstants.Broker.Request.QUERY_OPTIONS,
        String.format("%s=%s;%s=%s", CommonConstants.Broker.Request.QueryOptionKey.CURSOR_REQUEST_ID, requestId,
            CommonConstants.Broker.Request.QueryOptionKey.CURSOR_OFFSET, params.get("offset"))));

    if (params.containsKey("numRows")) {
      options.put(CommonConstants.Broker.Request.QUERY_OPTIONS,
          String.format("%s;%s=%s", options.get(CommonConstants.Broker.Request.QUERY_OPTIONS),
              CommonConstants.Broker.Request.QueryOptionKey.GET_CURSOR_NUM_ROWS, params.get("numRows"))
          );
    }

    return options;
  }

  protected void startHybridCluster()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServers(2);
    startKafka();

    // Create tenants
    getControllerRequestClient().createServerTenant(TENANT_NAME, 1, 1);
  }

  private List<CursorResponse> getAllResultPages(String queryResourceUrl, Map<String, String> headers,
      CursorResponse firstResponse, int numRows)
      throws Exception {
    numRows = numRows == 0 ? CommonConstants.CursorConfigs.DEFAULT_QUERY_RESULT_SIZE : numRows;

    int numPages =
        firstResponse.getNumRowsResultSet() / numRows + (firstResponse.getNumRowsResultSet() % numRows != 0 ? 1 : 0);
    List<CursorResponse> resultPages = new ArrayList<>(numPages);
    resultPages.add(firstResponse);

    String nextOffsetParams = firstResponse.getNextOffsetParams();
    for (int count = 1; count < numPages; count++) {
      JsonNode pinotResponse = ClusterTest.postQuery("", getBrokerQueryApiUrl(queryResourceUrl), headers,
          getCursorOffset(firstResponse.getRequestId(), nextOffsetParams));
      CursorResponse response = JsonUtils.jsonNodeToObject(pinotResponse, CursorResponseNative.class);
      resultPages.add(response);
      nextOffsetParams = response.getNextOffsetParams();
    }
    return resultPages;
  }

  private void compareNormalAndPagingApis(String pinotQuery, String queryResourceUrl,
      @Nullable Map<String, String> headers, @Nullable Map<String, String> extraJsonProperties)
      throws Exception {
    // broker response
    JsonNode pinotResponse;
    pinotResponse =
        ClusterTest.postQuery(pinotQuery, getBrokerQueryApiUrl(queryResourceUrl), headers, extraJsonProperties);
    if (!pinotResponse.get("exceptions").isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotResponse);
    }
    int brokerResponseSize = pinotResponse.get("numRowsResultSet").asInt();

    CursorResponse pinotPagingResponse;
    pinotPagingResponse = JsonUtils.jsonNodeToObject(
        ClusterTest.postQuery(pinotQuery, getBrokerQueryApiUrl(queryResourceUrl), headers, getCursorQueryProperties(_resultSize)), CursorResponseNative.class);
    if (!pinotPagingResponse.getExceptions().isEmpty()) {
      throw new RuntimeException(
          "Got Exceptions from Query Response: " + pinotPagingResponse.getExceptions().get(0));
    }
    List<CursorResponse> resultPages = getAllResultPages(queryResourceUrl, headers, pinotPagingResponse, _resultSize);

    int brokerPagingResponseSize = 0;
    for (CursorResponse response : resultPages) {
      brokerPagingResponseSize += response.getNumRows();
    }

    if (brokerResponseSize != brokerPagingResponseSize) {
      throw new RuntimeException(
          "Pinot # of rows from paging API " + brokerPagingResponseSize + " doesn't match # of rows from default API "
              + brokerResponseSize);
    }
  }

  protected Map<String, String> getHeaders() {
    return Collections.emptyMap();
  }

  @Override
  protected void testQuery(String pinotQuery, String h2Query)
      throws Exception {
    compareNormalAndPagingApis(pinotQuery, getBrokerBaseApiUrl(), getHeaders(), getExtraQueryProperties());
  }

  protected Object[][] getPageSizes() {
    return new Object[][]{
        {1}, {2}, {3}, {10}, {0} //0 trigger default behaviour
    };
  }

  @DataProvider(name = "pageSizeProvider")
  public Object[][] pageSizeProvider() {
    return getPageSizes();
  }

  @Test(dataProvider = "pageSizeProvider")
  public void testHardcodedQueries(int pageSize)
      throws Exception {
    _resultSize = pageSize;
    setUseMultiStageQueryEngine(false);
    notSupportedInV2();
    super.testHardcodedQueries();
  }

  @Test
  public void testCursorWorkflow()
      throws Exception {
    _resultSize = 10000;
    // Submit query
    CursorResponse pinotPagingResponse;
    JsonNode jsonNode = ClusterTest.postQuery(TEST_QUERY_THREE, getBrokerQueryApiUrl(getBrokerBaseApiUrl()), getHeaders(),
    getCursorQueryProperties(_resultSize));

    pinotPagingResponse = JsonUtils.jsonNodeToObject(jsonNode, CursorResponseNative.class);
    if (!pinotPagingResponse.getExceptions().isEmpty()) {
      throw new RuntimeException(
          "Got Exceptions from Query Response: " + pinotPagingResponse.getExceptions().get(0));
    }
    String requestId = pinotPagingResponse.getRequestId();

    Assert.assertFalse(pinotPagingResponse.getBrokerHost().isEmpty());
    Assert.assertTrue(pinotPagingResponse.getBrokerPort() > 0);
    Assert.assertTrue(pinotPagingResponse.getCursorFetchTimeMs() >= 0);
    Assert.assertTrue(pinotPagingResponse.getCursorResultWriteTimeMs() >= 0);

    while (pinotPagingResponse.getNextOffsetParams() != null) {
      JsonNode pinotResponse = ClusterTest.postQuery("", getBrokerQueryApiUrl(getBrokerBaseApiUrl()), getHeaders(),
          getCursorOffset(requestId, pinotPagingResponse.getNextOffsetParams()));
      pinotPagingResponse = JsonUtils.jsonNodeToObject(pinotResponse, CursorResponseNative.class);

      Assert.assertFalse(pinotPagingResponse.getBrokerHost().isEmpty());
      Assert.assertTrue(pinotPagingResponse.getBrokerPort() > 0);
      Assert.assertEquals(pinotPagingResponse.getCursorResultWriteTimeMs(), -1);
      Assert.assertTrue(pinotPagingResponse.getCursorFetchTimeMs() >= 0);
    }
    ClusterTest.sendDeleteRequest(getBrokerDeleteQueryStoresApiUrl(getBrokerBaseApiUrl(), requestId), getHeaders());
  }

  @Test
  public void testGetAndDelete()
      throws Exception {
    _resultSize = 100000;
    testQuery(TEST_QUERY_ONE);
    testQuery(TEST_QUERY_TWO);

    ResultResponse response = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        ResultResponse.class);

    Assert.assertEquals(response.getResultMetadataList().size(), 2);

    // Delete the first one
    String deleteRequestId = response.getResultMetadataList().get(0).getRequestId();
    ClusterTest.sendDeleteRequest(getBrokerDeleteQueryStoresApiUrl(getBrokerBaseApiUrl(), deleteRequestId),
        getHeaders());

    response = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        ResultResponse.class);

    Assert.assertEquals(response.getResultMetadataList().size(), 1);
    Assert.assertNotEquals(response.getResultMetadataList().get(0).getRequestId(), deleteRequestId);
  }

  @Test
  public void testBadGet() throws Exception {
    try {
      ClusterTest.postQuery("", getBrokerQueryApiUrl(getBrokerBaseApiUrl()), getHeaders(),
          getCursorOffset("dummy", "offset=0"));
    } catch (IOException e) {
      HttpErrorStatusException h = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(h.getStatusCode(), 404);
      Assert.assertTrue(h.getMessage().contains("Query results for dummy not found"));
    }
  }

  @Test
  public void testBadDelete() {
    try {
      ClusterTest.sendDeleteRequest(getBrokerDeleteQueryStoresApiUrl(getBrokerBaseApiUrl(), "dummy"), getHeaders());
    } catch (IOException e) {
      HttpErrorStatusException h = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(h.getStatusCode(), 404);
      Assert.assertTrue(h.getMessage().contains("Query results for dummy not found"));
    }
  }

  @Test
  public void testQueryWithEmptyResult()
      throws Exception {
    JsonNode pinotResponse;
    pinotResponse = ClusterTest.postQuery(EMPTY_RESULT_QUERY, getBrokerPagingQueryApiUrl(getBrokerBaseApiUrl(), 10000), getHeaders(),
        getExtraQueryProperties());
    // There should be no resultTable.
    Assert.assertTrue(pinotResponse.get("resultTable").isNull());
    // Total Rows in result set should be 0.
    Assert.assertEquals(pinotResponse.get("numRowsResultSet").asInt(), 0);
    // Rows in the current response should be 0
    Assert.assertEquals(pinotResponse.get("numRows").asInt(), 0);
    Assert.assertTrue(pinotResponse.get("exceptions").isEmpty());
  }

  @DataProvider(name = "InvalidOffsetQueryProvider")
  public Object[][] invalidOffsetQueryProvider() {
    return new Object[][]{{TEST_QUERY_ONE}, {EMPTY_RESULT_QUERY}};
  }

  @Test(dataProvider = "InvalidOffsetQueryProvider", expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Offset \\d+ is greater than totalRecords \\d+.*")
  public void testGetInvalidOffset(String query)
      throws Exception {
    String queryResourceUrl = getBrokerBaseApiUrl();

    CursorResponse pinotPagingResponse;
    pinotPagingResponse = JsonUtils.jsonNodeToObject(
        ClusterTest.postQuery(query, getBrokerPagingQueryApiUrl(queryResourceUrl, _resultSize), getHeaders(), null),
        CursorResponseNative.class);
    Assert.assertTrue(pinotPagingResponse.getExceptions().isEmpty());
    ClusterTest.postQuery("", getBrokerQueryApiUrl(getBrokerBaseApiUrl()), getHeaders(),
        getCursorOffset(pinotPagingResponse.getRequestId(), "offset=" + (pinotPagingResponse.getNumRowsResultSet() + 1)));
  }

  @Test
  public void testQueryWithRuntimeError()
      throws Exception {
    String queryWithFromMissing = "SELECT * mytable limit 100";
    JsonNode pinotResponse;
    pinotResponse =
        ClusterTest.postQuery(queryWithFromMissing, getBrokerPagingQueryApiUrl(getBrokerBaseApiUrl(), 10000),
            getHeaders(),
            getExtraQueryProperties());
    Assert.assertFalse(pinotResponse.get("exceptions").isEmpty());
    JsonNode exception = pinotResponse.get("exceptions").get(0);
    Assert.assertTrue(exception.get("message").asText().startsWith("QueryValidationError:"));
    Assert.assertEquals(exception.get("errorCode").asInt(), 700);
    Assert.assertTrue(pinotResponse.get("brokerId").asText().startsWith("Broker_"));
  }

  @Test
  public void testResultStoreCleaner()
      throws Exception {
    ResultResponse response = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        ResultResponse.class);

    int numQueryResults = response.getResultMetadataList().size();

    _resultSize = 100000;
    this.testQuery(TEST_QUERY_ONE);
    // Sleep so that both the queries do not have the same submission time.
    Thread.sleep(50);
    this.testQuery(TEST_QUERY_TWO);

    response = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        ResultResponse.class);

    int numQueryResultsAfter = response.getResultMetadataList().size();
    Assert.assertEquals(response.getResultMetadataList().size() - numQueryResults, 2);

    // Get the lower submission time.
    long expirationTime0 = response.getResultMetadataList().get(0).getExpirationTimeMs();
    long expirationTime1 = response.getResultMetadataList().get(1).getExpirationTimeMs();

    Properties perodicTaskProperties = new Properties();
    perodicTaskProperties.setProperty("requestId", "PaginationIntegrationTest");
    perodicTaskProperties.setProperty(ResultStoreCleaner.CLEAN_AT_TIME,
        Long.toString(Math.min(expirationTime0, expirationTime1)));
    _controllerStarter.getPeriodicTaskScheduler().scheduleNow("ResultStoreCleaner", perodicTaskProperties);

    // The periodic task is run in an executor thread. Give the thread some time to run the cleaner.
    TestUtils.waitForCondition(aVoid -> {
      try {
        ResultResponse getNumQueryResults = JsonUtils.stringToObject(
            ClusterTest.sendGetRequest(getBrokerGetAllQueryStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
            ResultResponse.class);
        return getNumQueryResults.getResultMetadataList().size() < numQueryResultsAfter;
      } catch (Exception e) {
        LOGGER.error(e.getMessage());
        return false;
      }
    }, 500L, 100_000L, "Failed to load delete query results", true);
  }
}
