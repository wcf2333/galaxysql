package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.orc.Writer;

import java.nio.channels.WritableByteChannel;
import java.util.List;

public class SqlCreateJavaFunction extends SqlDdl {

  private static final SqlSpecialOperator  OPERATOR = new SqlCreateJavaFunctionOperator();

  final SqlIdentifier                      funcName;
  protected String                         javaCode;
  protected String                         returnType;
  protected String                         inputType;

  public SqlCreateJavaFunction(SqlParserPos pos, SqlIdentifier funcName,
                               String returnType, String inputType, String javaCode) {
    super(OPERATOR, pos);
    this.funcName = funcName;
    this.returnType = returnType;
    this.inputType = inputType;
    this.javaCode = javaCode;
  }

  @Override
  public void unparse(SqlWriter writer, int lefPrec, int rightPrec) {
    writer.keyword("CREATE JAVA FUNCTION");

    funcName.unparse(writer, lefPrec, rightPrec);

    if (TStringUtil.isNotBlank(returnType)) {
      writer.keyword("RETURNTYPE");
      writer.literal(returnType);
    }
    if (TStringUtil.isNotBlank(inputType)) {
      writer.keyword("INPUTTYPE");
      writer.literal(inputType);
    }
    if (TStringUtil.isNotBlank(javaCode)) {
      writer.keyword("#CODE");
      writer.literal(javaCode);
      writer.keyword("#ENDCODE");
    }
  }

  public SqlIdentifier getFuncName() {
    return funcName;
  }

  public String getReturnType() {
    return returnType;
  }

  public String getInputType() {
    return inputType;
  }

  public String getJavaCode() {
    return javaCode;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of();
  }

  public static class SqlCreateJavaFunctionOperator extends SqlSpecialOperator {

      public SqlCreateJavaFunctionOperator() {
        super("CREATE_JAVA_FUNCTION", SqlKind.CREATE_JAVA_FUNCTION);
      }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
      final RelDataTypeFactory typeFactory = validator.getTypeFactory();
      final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

      return typeFactory.createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_JAVA_FUNCTION_RESULT",
          0,
          columnType)));
    }
  }
}