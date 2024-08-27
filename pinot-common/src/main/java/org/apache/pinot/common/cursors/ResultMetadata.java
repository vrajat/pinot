package org.apache.pinot.common.cursors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.JsonUtils;


public class ResultMetadata {
  private final List<ResponseMetadata> _responsesMetadata;
  private final String _brokerHost;
  private final int _brokerPort;
  private final String _requestId;
  private final String _brokerId;
  private final long _submissionTimeMs;
  private final long _expirationTimeMs;

  /**
   * Reads a metadata file from a location in PinotFs
   * @param pinotFS pinotFS object
   * @param metadataFile URI of the metadata file
   * @return Deserialized ResultMetadata object
   * @throws Exception
   */
  public static ResultMetadata read(PinotFS pinotFS, URI metadataFile)
      throws Exception {
    return JsonUtils.inputStreamToObject(pinotFS.open(metadataFile), ResultMetadata.class);
  }

  /**
   * Writes metadata to a file in a PinotFS file system
   * @param pinotFS PinotFS object
   * @param tempMetadataFile Path to a temporary file that the function can use
   * @param metadataFile URI of the final location of the metadata file.
   * @param metadata ResultMetadata object to write.
   * @throws Exception
   */
  public static void write(PinotFS pinotFS, Path tempMetadataFile, URI metadataFile, ResultMetadata metadata)
      throws Exception {
    try {
      Files.write(tempMetadataFile, JsonUtils.objectToBytes(metadata));
      pinotFS.copyFromLocalFile(tempMetadataFile.toFile(), metadataFile);
    } finally {
      Files.delete(tempMetadataFile);
    }
  }

  public ResultMetadata(@JsonProperty("brokerHost") String brokerHost, @JsonProperty("brokerPort") int brokerPort,
      @JsonProperty("requestId") String requestId, @JsonProperty("brokerId") String brokerId,
      @JsonProperty("submissionTimeMs") long submissionTimeMs, @JsonProperty("expirationTimeMs") long expirationTimeMs,
      @JsonProperty("responseMetadata") List<ResponseMetadata> metadataList) {
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _requestId = requestId;
    _brokerId = brokerId;
    _submissionTimeMs = submissionTimeMs;
    _expirationTimeMs = expirationTimeMs;
    _responsesMetadata = metadataList;
  }

  public void addResponseMetadata(ResponseMetadata responseMetadata) {
    _responsesMetadata.add(responseMetadata);
  }

  @JsonIgnore
  public int getNumResponses() {
    return _responsesMetadata.size();
  }

  @JsonProperty("responseMetadata")
  public List<ResponseMetadata> getResponsesMetadata() {
    return _responsesMetadata;
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
}
