package org.apache.pinot.broker.cursors.memory;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.broker.cursors.BaseQueryStore;
import org.apache.pinot.spi.cursors.ResultMetadata;
import org.apache.pinot.spi.query.QueryResponse;
import org.apache.pinot.spi.query.ResultSet;
import org.apache.pinot.spi.utils.JsonUtils;


public class MemoryQueryStore extends BaseQueryStore {
  private QueryResponse _response;
  private ResultSet _resultSet;
  private final ResultMetadata _resultMetadata;

  public MemoryQueryStore(ResultMetadata metadata) {
    _resultMetadata = metadata;
  }

  @Override
  public void setResponse(QueryResponse response) {
    _resultSet = response.getResultSet();
    response.setResultSet(null);
    _response = response;
  }

  @Override
  public ResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public ResultSet getResultSet() {
    return _resultSet;
  }

  @Override
  public JsonNode getQueryResponseMetadata() {
    return JsonUtils.objectToJsonNode(_response);
  }
}
