package org.apache.pinot.controller.helix.core.restream;

public class TableRestreamContext {
  private static final int INITIAL_ATTEMPT_ID = 1;
  private String _jobId;
  private String _originalJobId;
  private TableRestreamConfig _config;
  private int _attemptId;

  public static TableRestreamContext forInitialAttempt(String originalJobId, TableRestreamConfig config) {
    return new TableRestreamContext(originalJobId, config, INITIAL_ATTEMPT_ID);
  }

  public static TableRestreamContext forRetry(String originalJobId, TableRestreamConfig config, int attemptId) {
    return new TableRestreamContext(originalJobId, config, attemptId);
  }

  public TableRestreamContext() {
    // For JSON deserialization.
  }

  private TableRestreamContext(String originalJobId, TableRestreamConfig config, int attemptId) {
    _jobId = createAttemptJobId(originalJobId, attemptId);
    _originalJobId = originalJobId;
    _config = config;
    _attemptId = attemptId;
  }

  public int getAttemptId() {
    return _attemptId;
  }

  public void setAttemptId(int attemptId) {
    _attemptId = attemptId;
  }

  public String getOriginalJobId() {
    return _originalJobId;
  }

  public void setOriginalJobId(String originalJobId) {
    _originalJobId = originalJobId;
  }

  public String getJobId() {
    return _jobId;
  }

  public void setJobId(String jobId) {
    _jobId = jobId;
  }

  public TableRestreamConfig getConfig() {
    return _config;
  }

  public void setConfig(TableRestreamConfig config) {
    _config = config;
  }

  @Override
  public String toString() {
    return "TableRestreamContext{" + "_jobId='" + _jobId + '\'' + ", _originalJobId='" + _originalJobId + '\''
        + ", _config=" + _config + ", _attemptId=" + _attemptId + '}';
  }

  private static String createAttemptJobId(String originalJobId, int attemptId) {
    if (attemptId == INITIAL_ATTEMPT_ID) {
      return originalJobId;
    }
    return originalJobId + "_" + attemptId;
  }
}
