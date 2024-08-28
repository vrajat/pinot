package org.apache.pinot.spi.query;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


public interface QueryResponse {
  /**
   * Returns the result set.
   */
  @Nullable
  ResultSet getResultSet();

  /**
   * Sets the resultset. We expose this method to allow modifying the results on the client side, e.g. hiding the
   * results and only showing the stats.
   */
  void setResultSet(@Nullable ResultSet resultSet);

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
  List<? extends QueryException> getExceptions();
}
