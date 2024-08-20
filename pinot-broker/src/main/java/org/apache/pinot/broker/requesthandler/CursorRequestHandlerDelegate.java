package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.cursors.QueryStore;
import org.apache.pinot.broker.cursors.ResultCursor;
<<<<<<< HEAD
import org.apache.pinot.common.cursors.ResultMetadata;
=======
import org.apache.pinot.broker.cursors.ResultMetadata;
>>>>>>> 704a5e2bdc (Setup CursorRequestHandlerDelegate)
import org.apache.pinot.broker.cursors.ResultStore;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


public class CursorRequestHandlerDelegate {
  private final BrokerRequestHandler _brokerRequestHandler;
  private final ResultStore _resultStore;
  private final String _brokerHost;
  private final int _brokerPort;
  private final long _expirationIntervalInMs;

  public CursorRequestHandlerDelegate(String brokerHost, int brokerPort, ResultStore resultStore,
      BrokerRequestHandler brokerRequestHandler, String expirationTime) {
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _resultStore = resultStore;
    _brokerRequestHandler = brokerRequestHandler;
    _expirationIntervalInMs = TimeUtils.convertPeriodToMillis(expirationTime);
  }

  public CursorResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders,
      int numRows)
      throws Exception {
    BrokerResponse response =
        _brokerRequestHandler.handleRequest(request, sqlNodeAndOptions, requesterIdentity, requestContext, httpHeaders);

    if (response.getExceptionsSize() > 0) {
      return new CursorResponseNative(response.getBrokerId(), response.getExceptions());
    }

    long submissionTimeMs = System.currentTimeMillis();
    ResultMetadata metadata =
        new ResultMetadata(_brokerHost, _brokerPort, response.getRequestId(), response.getBrokerId(),
            submissionTimeMs, submissionTimeMs + _expirationIntervalInMs,
            new ArrayList<>());

    long cursorStoreStartTimeMs = System.currentTimeMillis();
    QueryStore queryStore = _resultStore.initQueryStore(metadata);
    queryStore.addResponse(response);
    long cursorStoreTimeMs = System.currentTimeMillis() - cursorStoreStartTimeMs;

    ResultCursor cursor = new ResultCursor(queryStore);
    cursor.seekAbsolute(0);
    CursorResponse cursorResponse = cursor.fetch(numRows);
    cursorResponse.setCursorResultWriteTimeMs(cursorStoreTimeMs);
    return cursorResponse;
  }
}
