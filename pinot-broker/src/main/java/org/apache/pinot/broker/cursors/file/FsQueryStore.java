package org.apache.pinot.broker.cursors.file;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.pinot.broker.cursors.BaseQueryStore;
import org.apache.pinot.spi.cursors.ResultMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.query.QueryResponse;
import org.apache.pinot.spi.query.ResultSet;
import org.apache.pinot.spi.utils.JsonUtils;


public class FsQueryStore extends BaseQueryStore {
  public static final String METADATA_FILENAME = "metadata.json";

  private final FsMetadata _metadata;
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
    if (!_pinotFS.exists(getMetadataFile(_queryDir))) {
      throw new RuntimeException("Metadata file for query id " + requestId + " not found");
    }
    _metadata = readMetadata();
  }

  @Override
  public void setResponse(QueryResponse response)
      throws Exception {
    Path tempResultTableFile =
        Utils.getTempPath(_localTempDir, "resultTable", _requestId);
    Path tempResponseFile = Utils.getTempPath(_localTempDir, "response", _requestId);

    try {
      Files.write(tempResultTableFile, JsonUtils.objectToBytes(response.getResultSet()));
      _pinotFS.copyFromLocalFile(tempResultTableFile.toFile(), _metadata.getDataFile());

      // Remove the resultTable from the response as it is serialized in a data file.
      response.setResultSet(null);
      Files.write(tempResponseFile, JsonUtils.objectToBytes(response));
      _pinotFS.copyFromLocalFile(tempResponseFile.toFile(), _metadata.getMetadataFile());
    } finally {
      Files.delete(tempResultTableFile);
      Files.delete(tempResponseFile);
    }
  }

  @Override
  public ResultMetadata getResultMetadata() {
    return _metadata.getResultMetadata();
  }

  @Override
  public ResultSet getResultSet()
      throws IOException {
    return JsonUtils.inputStreamToObject(_pinotFS.open(_metadata.getDataFile()), ResultSet.class);
  }

  @Override
  public JsonNode getQueryResponseMetadata() throws IOException {
      return JsonUtils.inputStreamToJsonNode(_pinotFS.open(_metadata.getMetadataFile()));
  }

  private FsMetadata readMetadata()
      throws Exception {
    return FsMetadata.read(_pinotFS, getMetadataFile(_queryDir));
  }
}
