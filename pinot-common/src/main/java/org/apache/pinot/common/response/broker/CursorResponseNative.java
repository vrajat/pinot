package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.cursors.CursorResponse;
import org.apache.pinot.spi.query.QueryException;
import org.apache.pinot.spi.query.ResultSet;


@JsonPropertyOrder({
    "resultTable", "requestId", "brokerId", "numRowsResultSet", "offset", "offset",
    "cursorResultWriteTimeMs", "cursorResultWriteTimeMs"
})
public class CursorResponseNative implements CursorResponse {
  private final String _requestId;
  private final String _brokerId;
  private final String _brokerHost;
  private final int _brokerPort;
  private ResultSet _resultSet;
  private final int _numRowsResultSet;
  private final int _offset;
  private final int _numRows;
  private final List<? extends QueryException> _processingExceptions;
  private long _cursorResultWriteTimeMs;
  private final long _cursorFetchTimeMs;

  public CursorResponseNative(QueryProcessingException exception) {
    _requestId = "NA";
    _brokerId = "NA";
    _brokerHost = "NA";
    _brokerPort = -1;
    _resultSet = null;
    _numRowsResultSet = 0;
    _offset = 0;
    _numRows = 0;
    _processingExceptions = List.of(exception);
    _cursorResultWriteTimeMs = -1;
    _cursorFetchTimeMs = -1;
  }

  public CursorResponseNative(String brokerId, List<? extends QueryException> exceptions) {
    _requestId = "NA";
    _brokerId = brokerId;
    _brokerHost = "NA";
    _brokerPort = -1;
    _resultSet = null;
    _numRowsResultSet = 0;
    _offset = 0;
    _numRows = 0;
    _processingExceptions = exceptions;
    _cursorResultWriteTimeMs = -1;
    _cursorFetchTimeMs = -1;
  }

  @JsonCreator
  public CursorResponseNative(@JsonProperty("requestId") String requestId, @JsonProperty("brokerId") String brokerId,
      @JsonProperty("brokerHost") String brokerHost, @JsonProperty("brokerPort") int brokerPort,
      @JsonProperty("resultTable") ResultSet resultSet, @JsonProperty("numRowsResultSet") int numRowsResultSet,
      @JsonProperty("offset") int offset, @JsonProperty("numRows") int numRows,
      @JsonProperty("exceptions") List<QueryProcessingException> exceptions,
      @JsonProperty("cursorResultWriteTimeMs") long cursorResultWriteTimeMs,
      @JsonProperty("cursorFetchTimeMs") long cursorFetchTimeMs) {
    _requestId = requestId;
    _brokerId = brokerId;
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _resultSet = resultSet;
    _numRowsResultSet = numRowsResultSet;
    _offset = offset;
    _numRows = numRows;
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
  @JsonProperty("resultTable")
  public ResultSet getResultSet() {
    return _resultSet;
  }

  @Override
  public void setResultSet(@Nullable ResultSet resultSet) {
    _resultSet = resultSet;
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

  @JsonProperty("exceptions")
  public List<? extends QueryException> getExceptions() {
    return _processingExceptions;
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
