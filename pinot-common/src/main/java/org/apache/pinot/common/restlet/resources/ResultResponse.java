package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.pinot.spi.cursors.ResultMetadata;


public class ResultResponse {
  private final List<ResultMetadata> _resultMetadataList;

  public ResultResponse(@JsonProperty("resultMetadataList") List<ResultMetadata> resultMetadataList) {
    _resultMetadataList = resultMetadataList;
  }

  @JsonProperty("resultMetadataList")
  public List<ResultMetadata> getResultMetadataList() {
    return _resultMetadataList;
  }
}
