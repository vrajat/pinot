package org.apache.pinot.broker.cursors.file;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.broker.cursors.BaseQueryStore;
import org.apache.pinot.common.cursors.ResponseMetadata;
import org.apache.pinot.common.cursors.ResultMetadata;
import org.apache.pinot.common.cursors.fs.FsMetadata;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.JsonUtils;


public class FsQueryStore extends BaseQueryStore {
  public static final String METADATA_FILENAME = "metadata.json";
  private static final String RESULT_TABLE_FILE_NAME_FORMAT = "resultTable_%d.json";
  private static final String RESPONSE_FILE_NAME_FORMAT = "response_%d.json";

  private final ResultMetadata _resultMetadata;
  private final PinotFS _pinotFS;
  private final String _requestId;
  private final Path _localTempDir;
  private final URI _queryDir;

  public static URI getMetadataFile(URI queryDir)
      throws URISyntaxException {
    return Utils.combinePath(queryDir, METADATA_FILENAME);
  }

  public FsQueryStore(PinotFS pinotFS, String requestId, Path localTempDir, URI queryDir)
      throws Exception {
    _pinotFS = pinotFS;
    _requestId = requestId;
    _localTempDir = localTempDir;
    _queryDir = queryDir;
    if (!_pinotFS.exists(Utils.combinePath(_queryDir, METADATA_FILENAME))) {
      throw new RuntimeException("Metadata file for query id " + requestId + " not found");
    }
    _resultMetadata = readMetadata();
  }

  @Override
  public void addResponse(BrokerResponse response)
      throws Exception {
    int responseIndex = _resultMetadata.getNumResponses();
    Path tempResultTableFile =
        Utils.getTempPath(_localTempDir, "resultTable", _requestId, Integer.toString(responseIndex));
    Path tempResponseFile = Utils.getTempPath(_localTempDir, "response", _requestId, Integer.toString(responseIndex));

    try {
      Files.write(tempResultTableFile, JsonUtils.objectToBytes(response.getResultTable()));
      URI resultTablePath = Utils.combinePath(_queryDir, String.format(RESULT_TABLE_FILE_NAME_FORMAT, responseIndex));
      _pinotFS.copyFromLocalFile(tempResultTableFile.toFile(), resultTablePath);

      // Remove the resultTable from the response as it is serialized in a data file.
      // Capture number rows as resultTable will be set to null.
      int numRows = response.getNumRowsResultSet();

      response.setResultTable(null);
      Files.write(tempResponseFile, JsonUtils.objectToBytes(response));
      URI responsePath = Utils.combinePath(_queryDir, String.format(RESPONSE_FILE_NAME_FORMAT, responseIndex));
      _pinotFS.copyFromLocalFile(tempResponseFile.toFile(), responsePath);

      _resultMetadata.addResponseMetadata(
          new FsMetadata(numRows, resultTablePath, responsePath));
      writeMetadata();
    } finally {
      Files.delete(tempResultTableFile);
      Files.delete(tempResponseFile);
    }
  }

  @Override
  public ResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public ResultTable getResultTable(int index)
      throws Exception {
    FsMetadata fsMetadata = (FsMetadata) _resultMetadata.getResponsesMetadata().get(index);
    return JsonUtils.inputStreamToObject(_pinotFS.open(fsMetadata.getDataFile()), ResultTable.class);
  }

  @Override
  public List<JsonNode> getBrokerResponses() throws IOException {
    List<JsonNode> responses = new ArrayList<>(_resultMetadata.getNumResponses());
    for (ResponseMetadata metadata : _resultMetadata.getResponsesMetadata()) {
      FsMetadata fsMetadata = (FsMetadata) metadata;
      responses.add(JsonUtils.inputStreamToJsonNode(_pinotFS.open(fsMetadata.getMetadataFile())));
    }
    return responses;
  }

  private ResultMetadata readMetadata()
      throws Exception {
    return ResultMetadata.read(_pinotFS, getMetadataFile(_queryDir));
  }

  private void writeMetadata()
      throws Exception {
    // Write a metadata file to temp dir and move to the query directory.
    Path tempMetadataFile = Utils.getTempPath(_localTempDir, _requestId, "metadata");
    ResultMetadata.write(_pinotFS, tempMetadataFile, getMetadataFile(_queryDir), _resultMetadata);
  }
}
