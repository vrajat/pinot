package org.apache.pinot.broker.cursors;

import org.apache.pinot.common.response.CursorResponse;


public class ResultCursor {
  private final QueryStore _queryStore;
  private int _offset;

  public ResultCursor(QueryStore queryStore) {
    _queryStore = queryStore;
    _offset = 0;
  }

  /**
   * Set the cursor to an absolute offset.
   * @param offset Offset (starting from zero) to seek to.
   */
  public void seekAbsolute(int offset) {
    _offset = offset;
  }

  /**
   * Get the current position of the cursor.
   * @return The current offset in the result array.
   */
  public int getOffset() {
    return _offset;
  }

  /**
   * Fetch a part of the result starting from the current position of the cursor.
   * @param numRows Number of rows to fetch.
   * @return Returns a BrokerResponse with rows starting from offset with numRows if available.
   */
  public CursorResponse fetch(int numRows)
      throws Exception {
    CursorResponse response = _queryStore.getResponse(_offset, numRows);
    _offset += numRows;
    return response;
  }
}
