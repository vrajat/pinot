package org.apache.pinot.common.cursors.fs;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.pinot.spi.cursors.ResultMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.JsonUtils;


public class FsMetadata {
  private final URI _dataFile;
  private final URI _metadataFile;
  private final ResultMetadata _resultMetadata;

  /**
   * Reads a metadata file from a location in PinotFs
   * @param pinotFS pinotFS object
   * @param metadataFile URI of the metadata file
   * @return Deserialized ResultMetadata object
   * @throws Exception
   */
  public static FsMetadata read(PinotFS pinotFS, URI metadataFile)
      throws Exception {
    return JsonUtils.inputStreamToObject(pinotFS.open(metadataFile), FsMetadata.class);
  }

  /**
   * Writes metadata to a file in a PinotFS file system
   * @param pinotFS PinotFS object
   * @param tempMetadataFile Path to a temporary file that the function can use
   * @param metadataFile URI of the final location of the metadata file.
   * @param metadata ResultMetadata object to write.
   * @throws Exception
   */
  public static void write(PinotFS pinotFS, Path tempMetadataFile, URI metadataFile, FsMetadata metadata)
      throws Exception {
    try {
      Files.write(tempMetadataFile, JsonUtils.objectToBytes(metadata));
      pinotFS.copyFromLocalFile(tempMetadataFile.toFile(), metadataFile);
    } finally {
      Files.delete(tempMetadataFile);
    }
  }

  public FsMetadata(@JsonProperty("dataFile") URI dataFile, @JsonProperty("metadataFile") URI metadataFile,
      @JsonProperty("resultMetadata") ResultMetadata resultMetadata) {
    _dataFile = dataFile;
    _metadataFile = metadataFile;
    _resultMetadata = resultMetadata;
  }

  @JsonProperty("dataFile")
  public URI getDataFile() {
    return _dataFile;
  }

  @JsonProperty("metadataFile")
  public URI getMetadataFile() {
    return _metadataFile;
  }

  @JsonProperty("resultMetadata")
  public ResultMetadata getResultMetadata() {
    return _resultMetadata;
  }
}
