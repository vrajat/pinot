package org.apache.pinot.spi.cursors;

import com.fasterxml.jackson.annotation.JsonProperty;


public class ResultMetadata {
  private final String _brokerHost;
  private final int _brokerPort;
  private final String _requestId;
  private final String _brokerId;
  private final long _submissionTimeMs;
  private final long _expirationTimeMs;
  private final int _numRowsResultSet;

  public ResultMetadata(@JsonProperty("brokerHost") String brokerHost, @JsonProperty("brokerPort") int brokerPort,
      @JsonProperty("requestId") String requestId, @JsonProperty("brokerId") String brokerId,
      @JsonProperty("submissionTimeMs") long submissionTimeMs, @JsonProperty("expirationTimeMs") long expirationTimeMs,
      @JsonProperty("numRowsResultSet") int numRowsResultSet) {
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _requestId = requestId;
    _brokerId = brokerId;
    _submissionTimeMs = submissionTimeMs;
    _expirationTimeMs = expirationTimeMs;
    _numRowsResultSet = numRowsResultSet;
  }

  @JsonProperty("requestId")
  public String getRequestId() {
    return _requestId;
  }

  @JsonProperty("submissionTimeMs")
  public long getSubmissionTimeMs() {
    return _submissionTimeMs;
  }

  @JsonProperty("expirationTimeMs")
  public long getExpirationTimeMs() {
    return _expirationTimeMs;
  }

  @JsonProperty("brokerId")
  public String getBrokerId() {
    return _brokerId;
  }

  public String getBrokerHost() {
    return _brokerHost;
  }

  public int getBrokerPort() {
    return _brokerPort;
  }

  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }
}
