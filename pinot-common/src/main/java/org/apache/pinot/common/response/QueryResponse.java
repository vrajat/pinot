package org.apache.pinot.common.response;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;


public interface QueryResponse {
  /**
   * Write the object JSON to the output stream.
   */
  default void toOutputStream(OutputStream outputStream)
      throws IOException {
    JsonUtils.objectToOutputStream(this, outputStream);
  }

  /**
   * Returns the processing exceptions encountered during the query execution.
   */
  List<QueryProcessingException> getExceptions();
}
