package org.apache.pinot.broker.cursors;

import java.util.Collections;
import org.apache.pinot.common.cursors.ResponseMetadata;
import org.apache.pinot.common.cursors.ResultMetadata;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;


public abstract class BaseQueryStore implements QueryStore {
  @Override
  public CursorResponse getResponse(int offset, int numRows)
      throws Exception {
    int sumRecords = 0;
    int index = 0;

    for (ResponseMetadata metadata : getResultMetadata().getResponsesMetadata()) {
      int totalTableRows = metadata.getNumRows();
      sumRecords += totalTableRows;
      if (offset < sumRecords) {
        long fetchStartTime = System.currentTimeMillis();
        ResultTable resultTable = getResultTable(index);
        return createCursorResponse(getResultMetadata(), resultTable, offset, numRows, totalTableRows,
            System.currentTimeMillis() - fetchStartTime);
      }
      index++;
    }

    // If sum records is 0, then result set is empty.
    if (sumRecords == 0 && offset == 0) {
      ResultMetadata metadata = getResultMetadata();
      return new CursorResponseNative(metadata.getRequestId(), metadata.getBrokerId(), metadata.getBrokerHost(),
          metadata.getBrokerPort(), null, 0, 0, 0, null, Collections.emptyList(), -1, -1);
    }

    throw new RuntimeException("Offset " + offset + " is greater than totalRecords " + sumRecords);
  }

  private static CursorResponse createCursorResponse(ResultMetadata metadata, ResultTable table, int offset,
      int numRows, int totalTableRows, long fetchTimeMs) {
    int sliceEnd = offset + numRows;
    String nextOffsetParams = "offset=" + sliceEnd + "&numRows=" + numRows;
    if (sliceEnd > totalTableRows) {
      sliceEnd = totalTableRows;
      numRows = sliceEnd - offset;
      nextOffsetParams = null;
    }

    return new CursorResponseNative(metadata.getRequestId(), metadata.getBrokerId(), metadata.getBrokerHost(),
        metadata.getBrokerPort(), new ResultTable(table.getDataSchema(), table.getRows().subList(offset, sliceEnd)),
        totalTableRows, offset, numRows, nextOffsetParams, Collections.emptyList(), -1,
        fetchTimeMs);
  }
}
