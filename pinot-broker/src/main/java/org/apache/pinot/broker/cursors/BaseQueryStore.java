package org.apache.pinot.broker.cursors;

import java.io.IOException;
import java.util.Collections;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.cursors.CursorResponse;
import org.apache.pinot.spi.cursors.QueryStore;
import org.apache.pinot.spi.cursors.ResultMetadata;
import org.apache.pinot.spi.query.ResultSet;


public abstract class BaseQueryStore implements QueryStore {
  protected abstract ResultSet getResultSet()
      throws IOException;

  @Override
  public CursorResponse getCursorResponse(int offset, int numRows)
      throws Exception {

    ResultMetadata resultMetadata = getResultMetadata();
    int totalTableRows = resultMetadata.getNumRowsResultSet();
    if (offset < totalTableRows) {
      long fetchStartTime = System.currentTimeMillis();
      ResultSet resultSet = getResultSet();
      return createCursorResponse(getResultMetadata(), resultSet, offset, numRows, totalTableRows,
            System.currentTimeMillis() - fetchStartTime);
    }

    // If sum records is 0, then result set is empty.
    if (totalTableRows == 0 && offset == 0) {
      return new CursorResponseNative(resultMetadata.getRequestId(), resultMetadata.getBrokerId(),
          resultMetadata.getBrokerHost(), resultMetadata.getBrokerPort(), null, 0, 0, 0, Collections.emptyList(), -1,
          -1);
    }

    throw new RuntimeException("Offset " + offset + " is greater than totalRecords " + totalTableRows);
  }

  private static CursorResponse createCursorResponse(ResultMetadata metadata, ResultSet table, int offset,
      int numRows, int totalTableRows, long fetchTimeMs) {
    int sliceEnd = offset + numRows;
    if (sliceEnd > totalTableRows) {
      sliceEnd = totalTableRows;
      numRows = sliceEnd - offset;
    }

    return new CursorResponseNative(metadata.getRequestId(), metadata.getBrokerId(), metadata.getBrokerHost(),
        metadata.getBrokerPort(),
        new ResultTable((DataSchema) table.getResultSchema(), table.getRows().subList(offset, sliceEnd)),
        totalTableRows, offset, numRows, Collections.emptyList(), -1, fetchTimeMs);
  }
}
