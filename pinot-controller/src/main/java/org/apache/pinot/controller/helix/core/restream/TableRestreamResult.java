package org.apache.pinot.controller.helix.core.restream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableRestreamResult {
  private final String _jobId;
  private final Status _status;
  private final Stage _stage;
  private final String _description;

  @JsonCreator
  public TableRestreamResult(@JsonProperty(value = "jobId", required = true) String jobId,
      @JsonProperty(value = "status", required = true) Status status,
      @JsonProperty(value = "stage", required = true) Stage stage,
      @JsonProperty(value = "description", required = true) String description) {
    _jobId = jobId;
    _status = status;
    _stage = stage;
    _description = description;
  }

  @JsonProperty
  public String getJobId() {
    return _jobId;
  }

  @JsonProperty
  public Status getStatus() {
    return _status;
  }

  @JsonProperty
  public Stage getStage() {
    return _stage;
  }

  @JsonProperty
  public String getDescription() {
    return _description;
  }

  public enum Status {
    // FAILED if the job has ended with known exceptions;
    // ABORTED if the job is stopped by others but retry is still allowed;
    // CANCELLED if the job is stopped by user, and retry is cancelled too;
    // UNKNOWN_ERROR if the job hits on an unexpected exception.
    NO_OP, DONE, FAILED, IN_PROGRESS, ABORTED, CANCELLED, UNKNOWN_ERROR
  }

  public enum Stage {
    INIT, TABLE_STREAM, UPSERT_SNAPSHOT, REBALANCE, SWITCH
  }
}
