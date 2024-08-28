package org.apache.pinot.spi.cursors;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.pinot.spi.query.QueryResponse;


/**
 * A query store represents storage for a specific query.
 * For example a specific query's storage maybe a directory in a file system or
 * the value in a map.
 */
public interface QueryStore {
  /**
   * Set a response for a query.
   * A query's result set maybe made up of many BrokerResponses. Add a response to the list of responses of the query.
   * It is assumed that the responses are added in the sequence it should be returned.
   * The implementation is expected to also manage any metadata associated with the response.
   * @param response BrokerResponse containing a partial result set.
   * @throws Exception
   */
  void setResponse(QueryResponse response)
      throws Exception;

  /**
   * Get a response that contains a specific offset.
   * @param offset Global offset in the complete result set of the query.
   * @param numRows Number of rows to return.
   * @return A BrokerResponse that contains the offset.
   * @throws Exception
   */
  CursorResponse getCursorResponse(int offset, int numRows)
      throws Exception;

  /**
   * Return metadata about the query result in QueryStore object
   * @return Metadata object
   */
  ResultMetadata getResultMetadata()
      throws Exception;

  /**
   * Get QueryResponse without the ResultSet.
   * Note that JsonNode objects are returned since serialized versions do not have type information to
   * instantiate an implementation of QueryResponse interface.
   * @return A JsonNode object of QueryResponse object
   */
  JsonNode getQueryResponseMetadata()
      throws IOException;
}
