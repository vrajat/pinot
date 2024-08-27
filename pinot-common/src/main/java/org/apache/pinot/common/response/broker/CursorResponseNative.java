package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import org.apache.pinot.common.response.CursorResponse;


@JsonPropertyOrder({
    "resultTable", "requestId", "brokerId", "numRowsResultSet", "offset", "offset", "nextOffsetParams",
    "cursorResultWriteTimeMs", "cursorResultWriteTimeMs"
})
public class CursorResponseNative implements CursorResponse {
  private final String _requestId;
  private final String _brokerId;
  private final String _brokerHost;
  private final int _brokerPort;
  private final ResultTable _resultTable;
  private final int _numRowsResultSet;
  private final int _offset;
  private final int _numRows;
  private final String _nextOffsetParams;
  private final List<QueryProcessingException> _processingExceptions;
  private long _cursorResultWriteTimeMs;
  private final long _cursorFetchTimeMs;

  public CursorResponseNative(QueryProcessingException exception) {
    _requestId = "NA";
    _brokerId = "NA";
    _brokerHost = "NA";
    _brokerPort = -1;
    _resultTable = null;
    _numRowsResultSet = 0;
    _offset = 0;
    _numRows = 0;
    _nextOffsetParams = "NA";
    _processingExceptions = List.of(exception);
    _cursorResultWriteTimeMs = -1;
    _cursorFetchTimeMs = -1;
  }

  public CursorResponseNative(String brokerId, List<QueryProcessingException> exceptions) {
    _requestId = "NA";
    _brokerId = brokerId;
    _brokerHost = "NA";
    _brokerPort = -1;
    _resultTable = null;
    _numRowsResultSet = 0;
    _offset = 0;
    _numRows = 0;
    _nextOffsetParams = "NA";
    _processingExceptions = exceptions;
    _cursorResultWriteTimeMs = -1;
    _cursorFetchTimeMs = -1;
  }

  @JsonCreator
  public CursorResponseNative(@JsonProperty("requestId") String requestId, @JsonProperty("brokerId") String brokerId,
      @JsonProperty("brokerHost") String brokerHost, @JsonProperty("brokerPort") int brokerPort,
      @JsonProperty("resultTable") ResultTable resultTable, @JsonProperty("numRowsResultSet") int numRowsResultSet,
      @JsonProperty("offset") int offset, @JsonProperty("numRows") int numRows,
      @JsonProperty("nextOffsetParams") String nextOffsetParams,
      @JsonProperty("exceptions") List<QueryProcessingException> exceptions,
      @JsonProperty("cursorResultWriteTimeMs") long cursorResultWriteTimeMs,
      @JsonProperty("cursorFetchTimeMs") long cursorFetchTimeMs) {
    _requestId = requestId;
    _brokerId = brokerId;
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _resultTable = resultTable;
    _numRowsResultSet = numRowsResultSet;
    _offset = offset;
    _numRows = numRows;
    _nextOffsetParams = nextOffsetParams;
    _processingExceptions = exceptions;
    _cursorResultWriteTimeMs = cursorResultWriteTimeMs;
    _cursorFetchTimeMs = cursorFetchTimeMs;
  }

  @Override
  public String getRequestId() {
    return _requestId;
  }

  @Override
  public String getBrokerId() {
    return _brokerId;
  }

  @Override
  public String getBrokerHost() {
    return _brokerHost;
  }

  @Override
  public int getBrokerPort() {
    return _brokerPort;
  }

  @Override
  public ResultTable getResultTable() {
    return _resultTable;
  }

  @Override
  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  @Override
  public int getOffset() {
    return _offset;
  }

  @Override
  public int getNumRows() {
    return _numRows;
  }

  @Override
  public String getNextOffsetParams() {
    return _nextOffsetParams;
  }

  @JsonProperty("exceptions")
  public List<QueryProcessingException> getExceptions() {
    return this._processingExceptions;
  }

  @JsonProperty("cursorResultWriteTimeMs")
  public long getCursorResultWriteTimeMs() {
    return _cursorResultWriteTimeMs;
  }

  @JsonIgnore
  @Override
  public void setCursorResultWriteTimeMs(long cursorResultWriteMs) {
    _cursorResultWriteTimeMs = cursorResultWriteMs;
  }

  @JsonProperty("cursorFetchTimeMs")
  public long getCursorFetchTimeMs() {
    return _cursorFetchTimeMs;
  }
}
