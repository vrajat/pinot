package org.apache.pinot.broker.cursors.memory;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.broker.cursors.BaseQueryStore;
import org.apache.pinot.common.cursors.ResponseMetadata;
import org.apache.pinot.common.cursors.ResultMetadata;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.utils.JsonUtils;


public class MemoryQueryStore extends BaseQueryStore {
  private final List<ResultTable> _resultTables;
  private final List<BrokerResponse> _responses;
  private final ResultMetadata _resultMetadata;

  public MemoryQueryStore(ResultMetadata metadata) {
    _resultTables = new ArrayList<>();
    _responses = new ArrayList<>();
    _resultMetadata = metadata;
  }

  @Override
  public void addResponse(BrokerResponse response) {
    _resultTables.add(response.getResultTable());
    // Capture number rows as resultTable will be set to null.
    int numRows = response.getNumRowsResultSet();

    response.setResultTable(null);
    _responses.add(response);
    _resultMetadata.addResponseMetadata(new ResponseMetadata(numRows));
  }

  @Override
  public ResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public ResultTable getResultTable(int index)
      throws IndexOutOfBoundsException {
    return _resultTables.get(index);
  }

  @Override
  public List<JsonNode> getBrokerResponses() {
    return _responses.stream().map(JsonUtils::objectToJsonNode).collect(Collectors.toList());
  }
}
