package org.apache.pinot.common.cursors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.pinot.common.cursors.fs.FsMetadata;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = FsMetadata.class, name = "fs"), @JsonSubTypes.Type(value = ResponseMetadata.class,
    name = "base")
})
public class ResponseMetadata {
  private final int _numRows;

  public ResponseMetadata(@JsonProperty("numRows") int numRows) {
    _numRows = numRows;
  }

  @JsonProperty("numRows")
  public int getNumRows() {
    return _numRows;
  }
}
