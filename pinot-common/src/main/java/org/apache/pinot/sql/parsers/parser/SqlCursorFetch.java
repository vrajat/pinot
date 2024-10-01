package org.apache.pinot.sql.parsers.parser;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;


public class SqlCursorFetch extends SqlCall {
  private static final SqlOperator FETCH_OPERATOR;

  private final SqlNode _requestId;
  private final SqlNumericLiteral _offset;
  private final SqlNumericLiteral _numRows;

  public SqlCursorFetch(SqlParserPos pos, SqlNode requestId, SqlNumericLiteral offset, SqlNumericLiteral numRows) {
    super(pos);
    _requestId = requestId;
    _offset = offset;
    _numRows = numRows;
  }

  @Override
  public SqlOperator getOperator() {
    return FETCH_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(_requestId, _offset, _numRows);
  }

  static {
    FETCH_OPERATOR = new SqlSpecialOperator("FETCH CURSOR", SqlKind.CURSOR);
  }
}
