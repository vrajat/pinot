package org.apache.pinot.spi.query;

import java.util.List;


public interface ResultSet {
  ResultSchema getResultSchema();
  List<Object[]> getRows();
}
