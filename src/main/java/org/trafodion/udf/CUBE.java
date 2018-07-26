package org.trafodion.udf;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.trafodion.sql.udr.ColumnInfo;
import org.trafodion.sql.udr.TypeInfo;
import org.trafodion.sql.udr.UDR;
import org.trafodion.sql.udr.UDRException;
import org.trafodion.sql.udr.UDRInvocationInfo;
import org.trafodion.sql.udr.UDRPlanInfo;

//SELECT SUM(SAL),CITY,DEPT FROM (SELECT SUM(SAL) AS SAL ,CITY,DEPT FROM T2 GROUP BY CITY,DEPT) T GROUP BY CITY,DEPT;

//SELECT SUM(A),CITY,DEPT FROM UDF(MYCUBE('SELECT SAL,CITY,DEPT FROM T2','SUM(SAL), AVG(SAL)','CITY,DEPT')) T GROUP BY CITY,DEPT;

//select sum(sal), avg(sal), city, dept from t2 group by city, dept;
//sum(sal)              avg(sal)              CITY  DEPT
//--------------------  --------------------  ----  ----
//
//               18000                  6000  BJ    3   
//               15000                  5000  BJ    2   
//               12000                  4000  BJ    1   
//                3600                  1800  SH    3   
//                3400                  1700  SH    2   
//                3200                  1600  SH    1 

//select sum(sal), avg(sal), city from (query) t group by city;
//sum(sal)              avg(sal)              CITY
//--------------------  --------------------  ----
//
//               45000                  5000  BJ  
//               10200                  1700  SH  

//select sum(sal), avg(sal), dept from (query) t group by dept;
//sum(sal)              avg(sal)              DEPT
//--------------------  --------------------  ----
//
//               21600                  4320  3   
//               18400                  3680  2   
//               15200                  3040  1  

public class CUBE extends UDR {
  static class JdbcConnectionInfo {
    static String trafHome;
    static String javaHome;
    static String classPath;

    static String driverJar_;
    static String driverClassName_ = "org.trafodion.jdbc.t2.T2Driver";
    String connectionString_ = "jdbc:t2jdbc:";
    boolean debug_;

    Connection conn_;
    static {
      init();
    }

    private static void init() {
      try {
        Class.forName(driverClassName_);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      trafHome = System.getenv().get("MY_SQROOT");// for version before 2.3
      if (trafHome == null || trafHome.length() == 0) {
        trafHome = System.getenv("TRAF_HOME");// for version after 2.3
      }
      javaHome = System.getenv().get("JAVA_HOME");// maybe need version check in the future
      classPath = System.getenv().get("CLASSPATH");// maybe need do some filter in the future

      driverJar_ = trafHome + File.separator + "export" + File.separator + "lib" + File.separator
          + "jdbcT2.jar";
    }

    public JdbcConnectionInfo(Properties prop) throws UDRException {
      try {

        conn_ = DriverManager.getConnection(connectionString_, prop);
      } catch (SQLException se) {
        throw new UDRException(38020, "SQL exception during connect. Message: %s", se.getMessage());
      } catch (Exception e) {
        if (debug_) {
          System.out.println("Debug: Exception during connect:");
          try {
            e.printStackTrace(System.out);
          } catch (Exception e2) {
          }
        }
        throw new UDRException(38020, "Exception during connect: %s", e.getMessage());
      }
    }

    public Connection getConnection() {
      return conn_;
    }

    public void disconnect() throws SQLException {
      if (conn_ != null) {
        conn_.close();
      }
      conn_ = null;
    }
  };

  private static String SELECT = "SELECT ";
  private static String FROM = "FROM ";

  String parseQuery(String query, String... cols) {
    StringBuffer sb = new StringBuffer();
    sb.append(SELECT);
    for (int i = 0; i < cols.length; i++) {
      sb.append(cols[i]);
      if (i != cols.length - 1) {
        sb.append(", ");
      } else {
        sb.append(" ");
      }
    }
    sb.append(FROM).append("(").append(query).append(") t group by ");
    for (int i = 1; i < cols.length; i++) {
      sb.append(cols[i]);
      if (i != cols.length - 1) {
        sb.append(", ");
      } else {
        sb.append(";");
      }
    }

    return sb.toString();
  }

  private static List<String> defColArr =
      Arrays.asList(new String[] { "A", "B", "C", "D", "E", "F", "G", "H" });

  List<String> getAggreCol(String param) {
    String[] colArr = param.split(",");
    List<String> list = new ArrayList<String>();
    for (int i = 0; i < colArr.length; i++) {
      String col = colArr[i].toUpperCase();
      // String fun = col.substring(0, col.indexOf("(")).trim();
      // String cName = col.substring(col.indexOf("(") + 1,
      // col.lastIndexOf(")")).trim();
      String aName = null;
      if (col.indexOf(" as") != -1) {
        aName = col.substring(col.indexOf("as") + 2).trim();
      }
      if (aName == null) {
        aName = defColArr.get(i);
      }
      list.add(aName);
    }
    return list;
  }

  Map<String, Integer> genColAndIndexMap(List<String> cols) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (int i = 0; i < cols.size(); i++) {
      map.put(cols.get(i), i + 1);
    }
    return map;
  }

  List<List<String>> results = new ArrayList<List<String>>();

  public <E> void combinerSelect(List<E> data, List<E> workSpace, int n, int k,
      List<List<String>> result) {
    List<E> copyData;
    List<E> copyWorkSpace;

    if (workSpace.size() == k) {
      List<String> tmp = new ArrayList<String>();
      for (Object c : workSpace) {
        tmp.add(c.toString());
      }
      result.add(tmp);
    }

    for (int i = 0; i < data.size(); i++) {
      copyData = new ArrayList<E>(data);
      copyWorkSpace = new ArrayList<E>(workSpace);

      copyWorkSpace.add(copyData.get(i));
      for (int j = i; j >= 0; j--)
        copyData.remove(j);
      combinerSelect(copyData, copyWorkSpace, n, k, result);
    }

  }

  TypeInfo getUDRTypeFromJDBCType(ResultSetMetaData desc, int colNumOneBased) throws UDRException {
    TypeInfo result;

    final int maxLength = 100000;

    int colJDBCType;

    TypeInfo.SQLTypeCode sqlType = TypeInfo.SQLTypeCode.UNDEFINED_SQL_TYPE;
    int length = 0;
    boolean nullable = false;
    int scale = 0;
    TypeInfo.SQLCharsetCode charset = TypeInfo.SQLCharsetCode.CHARSET_UCS2;
    TypeInfo.SQLIntervalCode intervalCode = TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE;
    int precision = 0;
    TypeInfo.SQLCollationCode collation = TypeInfo.SQLCollationCode.SYSTEM_COLLATION;

    try {
      colJDBCType = desc.getColumnType(colNumOneBased);
      nullable = (desc.isNullable(colNumOneBased) != ResultSetMetaData.columnNoNulls);
      switch (colJDBCType) {
      case java.sql.Types.SMALLINT:
      case java.sql.Types.TINYINT:
      case java.sql.Types.BOOLEAN:
        if (desc.isSigned(colNumOneBased)) sqlType = TypeInfo.SQLTypeCode.SMALLINT;
        else sqlType = TypeInfo.SQLTypeCode.SMALLINT_UNSIGNED;
        break;
      case java.sql.Types.INTEGER:
        if (desc.isSigned(colNumOneBased)) sqlType = TypeInfo.SQLTypeCode.INT;
        else sqlType = TypeInfo.SQLTypeCode.INT_UNSIGNED;
        break;
      case java.sql.Types.BIGINT:
        sqlType = TypeInfo.SQLTypeCode.LARGEINT;
        break;
      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC:
        if (desc.isSigned(colNumOneBased)) sqlType = TypeInfo.SQLTypeCode.NUMERIC;
        else sqlType = TypeInfo.SQLTypeCode.NUMERIC_UNSIGNED;
        precision = desc.getPrecision(colNumOneBased);
        scale = desc.getScale(colNumOneBased);
        break;
      case java.sql.Types.REAL:
        sqlType = TypeInfo.SQLTypeCode.REAL;
        break;
      case java.sql.Types.DOUBLE:
      case java.sql.Types.FLOAT:
        sqlType = TypeInfo.SQLTypeCode.DOUBLE_PRECISION;
        break;
      case java.sql.Types.CHAR:
      case java.sql.Types.NCHAR:
        sqlType = TypeInfo.SQLTypeCode.CHAR;
        length = Math.min(desc.getPrecision(colNumOneBased), maxLength);
        charset = TypeInfo.SQLCharsetCode.CHARSET_UCS2;
        break;
      case java.sql.Types.VARCHAR:
      case java.sql.Types.NVARCHAR:
        sqlType = TypeInfo.SQLTypeCode.VARCHAR;
        length = Math.min(desc.getPrecision(colNumOneBased), maxLength);
        charset = TypeInfo.SQLCharsetCode.CHARSET_UCS2;
        break;
      case java.sql.Types.DATE:
        sqlType = TypeInfo.SQLTypeCode.DATE;
        break;
      case java.sql.Types.TIME:
        sqlType = TypeInfo.SQLTypeCode.TIME;
        break;
      case java.sql.Types.TIMESTAMP:
        sqlType = TypeInfo.SQLTypeCode.TIMESTAMP;
        scale = 3;
        break;
      case java.sql.Types.ARRAY:
      case java.sql.Types.BINARY:
      case java.sql.Types.BIT:
      case java.sql.Types.BLOB:
      case java.sql.Types.DATALINK:
      case java.sql.Types.DISTINCT:
      case java.sql.Types.JAVA_OBJECT:
      case java.sql.Types.LONGVARBINARY:
      case java.sql.Types.NULL:
      case java.sql.Types.OTHER:
      case java.sql.Types.REF:
      case java.sql.Types.STRUCT:
      case java.sql.Types.VARBINARY:
        sqlType = TypeInfo.SQLTypeCode.VARCHAR;
        length = Math.min(desc.getPrecision(colNumOneBased), maxLength);
        charset = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;
        break;
      case java.sql.Types.LONGVARCHAR:
      case java.sql.Types.LONGNVARCHAR:
      case java.sql.Types.CLOB:
      case java.sql.Types.NCLOB:
      case java.sql.Types.ROWID:
      case java.sql.Types.SQLXML:
        sqlType = TypeInfo.SQLTypeCode.VARCHAR;
        length = Math.min(desc.getPrecision(colNumOneBased), maxLength);
        charset = TypeInfo.SQLCharsetCode.CHARSET_UCS2;
        break;
      }
    } catch (SQLException e) {
      throw new UDRException(38500, "Error determinging the type of output column %d: ",
          colNumOneBased, e.getMessage());
    }

    result =
        new TypeInfo(sqlType, length, nullable, scale, charset, intervalCode, precision, collation);

    return result;
  }

  @Override
  public void describeParamsAndColumns(UDRInvocationInfo info) throws UDRException {
    super.describeParamsAndColumns(info);

    this.results.clear();
    int paramNum = info.par().getNumColumns();
    if (paramNum < 3) {
      throw new UDRException(38310, "Expecting at least 3 parameters for %s UDR",
          info.getUDRName());
    }
    // String query = info.par().getString(0);
    String aggreCol = info.par().getString(1);
    List<String> aggreColList = getAggreCol(aggreCol);
    for (int i = 0; i < aggreColList.size(); i++) {
      String colName = aggreColList.get(i).toUpperCase();
      // info.out().addVarCharColumn(colName, 100, true);
      TypeInfo udrType = new TypeInfo(TypeInfo.SQLTypeCode.NUMERIC, 0, true, 2,
          TypeInfo.SQLCharsetCode.CHARSET_UCS2, TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE,
          18, TypeInfo.SQLCollationCode.SYSTEM_COLLATION);
      info.out().addColumn(new ColumnInfo(colName, udrType));
    }

    List<String> groupByCols =
        Arrays.asList(info.par().getString(2).replaceAll(" ", "").split(","));
    for (int i = 0; i < groupByCols.size(); i++) {
      String colName = groupByCols.get(i).toUpperCase();
      info.out().addVarCharColumn(colName, 100, true);
    }

    for (int i = 1; i <= groupByCols.size(); i++)
      combinerSelect(groupByCols, new ArrayList<String>(), groupByCols.size(), i, this.results);

    Properties prop = new Properties();
    prop.put("schema", "SEABASE");
    prop.put("catalog", "TRAFODION");
    // JdbcConnectionInfo t2 = new JdbcConnectionInfo(prop);
    // JdbcConnectionInfo t2 = new JdbcConnectionInfo();
    // Connection conn = t2.getConnection();
    // try {
    // System.out.println("do prepareStatement for query : " + query);
    // PreparedStatement preparedStmt = conn.prepareStatement(query);
    // if (preparedStmt == null)
    // throw new UDRException(38310, "syntax error in query %s UDR", query);
    // else
    // preparedStmt.close();
    // } catch (SQLException e) {
    // try {
    // t2.disconnect();
    // } catch (SQLException e1) {
    // e1.printStackTrace();
    // }
    // throw new UDRException(e.getErrorCode(), e.getMessage());
    // }
    //
    // for (List<String> colList : this.results) {
    // colList.add(0, aggreCol);
    // String realQuery = parseQuery(query, colList.toArray(new
    // String[colList.size()]));
    // try {
    // System.out.println("do prepareStatement for realQuery : " +
    // realQuery);
    // PreparedStatement preparedStmt2 = conn.prepareStatement(realQuery);
    // if (preparedStmt2 == null)
    // throw new UDRException(38310, "syntax error in query %s UDR",
    // realQuery);
    // else
    // preparedStmt2.close();
    // } catch (SQLException e) {
    // try {
    // t2.disconnect();
    // } catch (SQLException e1) {
    // e1.printStackTrace();
    // }
    // throw new UDRException(e.getErrorCode(), e.getMessage());
    // }
    // }
    // try {
    // t2.disconnect();
    // } catch (SQLException e1) {
    // e1.printStackTrace();
    // }
  }

  @Override
  public void processData(UDRInvocationInfo info, UDRPlanInfo plan) throws UDRException {
    this.results.clear();
    int numCols = info.out().getNumColumns();

    String query = info.par().getString(0);
    String aggreCol = info.par().getString(1);

    List<String> groupByCols =
        Arrays.asList(info.par().getString(2).replaceAll(" ", "").split(","));

    for (int i = 1; i <= groupByCols.size(); i++)
      combinerSelect(groupByCols, new ArrayList<String>(), groupByCols.size(), i, this.results);
    List<String> rsCols = null;
    Map<String, Integer> map = null;

    Properties prop = new Properties();
    prop.put("schema", "SEABASE");
    prop.put("catalog", "TRAFODION");
    JdbcConnectionInfo t2 = new JdbcConnectionInfo(prop);
    try {
      Connection conn = t2.getConnection();
      Statement stmt = conn.createStatement();
      for (List<String> colList : this.results) {
        rsCols = getAggreCol(aggreCol);
        rsCols.addAll(colList);
        colList.add(0, aggreCol);
        String realQuery = parseQuery(query, colList.toArray(new String[colList.size()]));
        map = genColAndIndexMap(rsCols);
        if (stmt.execute(realQuery)) {
          ResultSet rs = stmt.getResultSet();
          while (rs.next()) {
            for (int c = 0; c < numCols; c++) {
              String colName = info.out().getColumn(c).getColName();
              if (map.containsKey(colName)) {
                String val = rs.getString(map.get(colName));
                info.out().setString(c, val);
              } else {
                info.out().setString(c, "");
              }
            }

            emitRow(info);

          }
        }
      }
    } catch (SQLException e1) {
      e1.printStackTrace();
    }

  }

}
