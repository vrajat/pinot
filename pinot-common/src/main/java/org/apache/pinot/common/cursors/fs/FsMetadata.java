package org.apache.pinot.common.cursors.fs;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import org.apache.pinot.common.cursors.ResponseMetadata;


public class FsMetadata extends ResponseMetadata {
  private final URI _dataFile;
  private final URI _metadataFile;

  public FsMetadata(@JsonProperty("numRows") int numRows, @JsonProperty("dataFile") URI dataFile,
      @JsonProperty("metadataFile") URI metadataFile) {
    super(numRows);
    _dataFile = dataFile;
    _metadataFile = metadataFile;
  }

  @JsonProperty("dataFile")
  public URI getDataFile() {
    return _dataFile;
  }

  @JsonProperty("metadataFile")
  public URI getMetadataFile() {
    return _metadataFile;
  }
}
