package org.apache.pinot.spi.query;

public interface QueryException {
  int getErrorCode();
  String getMessage();
}
