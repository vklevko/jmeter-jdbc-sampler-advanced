package com.redhat.jmeter.sampler.jdbc;

import org.apache.commons.collections.map.AbstractLinkedMap;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.jmeter.protocol.jdbc.AbstractJDBCTestElement;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by vklevko on 2/29/16.
 */
public abstract class AbstractJDBCAdvancedTestElement extends AbstractJDBCTestElement {

    private static final Logger log = LoggingManager.getLoggerForClass();

    private String fetchSize = "10";
    private String resultSetReadDelay = "0";

    static final String SELECT = "Select Statement";
    static final String CALLABLE = "Callable Statement"; // $NON-NLS-1$
    static final String PREPARED_SELECT = "Prepared Select Statement"; // $NON-NLS-1$
    private static final String COMMA = ","; // $NON-NLS-1$
    private static final char COMMA_CHAR = ',';
    private static final String UNDERSCORE = "_"; // $NON-NLS-1$

    private static final String OUT = "OUT"; // $NON-NLS-1$
    private static final String INOUT = "INOUT"; // $NON-NLS-1$

    private static final String NULL_MARKER =
            JMeterUtils.getPropDefault("jdbcsampler.nullmarker", "]NULL["); // $NON-NLS-1$

    private static final int MAX_OPEN_PREPARED_STATEMENTS =
            JMeterUtils.getPropDefault("jdbcsampler.maxopenpreparedstatements", 100);

    static final String RS_STORE_AS_STRING = "Store as String"; // $NON-NLS-1$
    static final String RS_STORE_AS_OBJECT = "Store as Object"; // $NON-NLS-1$
    static final String RS_COUNT_RECORDS = "Count Records"; // $NON-NLS-1$

    private String resultSetHandler = RS_STORE_AS_STRING;

    private static final Map<String, Integer> mapJdbcNameToInt;
    // read-only after class init

    static {
        // based on e291. Getting the Name of a JDBC Type from javaalmanac.com
        // http://javaalmanac.com/egs/java.sql/JdbcInt2Str.html
        mapJdbcNameToInt = new HashMap<String, Integer>();

        //Get all fields in java.sql.Types and store the corresponding int values
        Field[] fields = java.sql.Types.class.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                String name = fields[i].getName();
                Integer value = (Integer) fields[i].get(null);
                mapJdbcNameToInt.put(name.toLowerCase(java.util.Locale.ENGLISH), value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e); // should not happen
            }
        }
    }

    /**
     * Cache of PreparedStatements stored in a per-connection basis. Each entry of this
     * cache is another Map mapping the statement string to the actual PreparedStatement.
     * At one time a Connection is only held by one thread
     */
    private static final Map<Connection, Map<String, PreparedStatement>> perConnCache =
            new ConcurrentHashMap<Connection, Map<String, PreparedStatement>>();

    public AbstractJDBCAdvancedTestElement() {
        super();
    }

    @Override
    protected byte[] execute(Connection conn) throws SQLException, UnsupportedEncodingException, IOException, UnsupportedOperationException {
        log.debug("executing jdbc");
        Statement stmt = null;

        try {
            // Based on query return value, get results
            String _queryType = getQueryType();
            if (SELECT.equals(_queryType)) {
                stmt = conn.createStatement();
                stmt.setQueryTimeout(getIntegerQueryTimeout());
                stmt.setFetchSize(NumberUtils.isNumber(getFetchSize()) ? Integer.valueOf(getFetchSize()) : 10);
                ResultSet rs = null;
                try {
                    rs = stmt.executeQuery(getQuery());
                    return getStringFromResultSet(rs).getBytes(ENCODING);
                } finally {
                    close(rs);
                }
            } else if (CALLABLE.equals(_queryType)) {
                CallableStatement cstmt = getCallableStatement(conn);
                cstmt.setFetchSize(NumberUtils.isNumber(getFetchSize()) ? Integer.valueOf(getFetchSize()) : 10);
                int out[] = setArguments(cstmt);
                // A CallableStatement can return more than 1 ResultSets
                // plus a number of update counts.
                boolean hasResultSet = cstmt.execute();
                String sb = resultSetsToString(cstmt, hasResultSet, out);
                return sb.getBytes(ENCODING);
            }
            else if (PREPARED_SELECT.equals(_queryType)) {
                PreparedStatement pstmt = getPreparedStatement(conn);
                pstmt.setFetchSize(NumberUtils.isNumber(getFetchSize()) ? Integer.valueOf(getFetchSize()) : 10);
                setArguments(pstmt);
                ResultSet rs = null;
                try {
                    rs = pstmt.executeQuery();
                    return getStringFromResultSet(rs).getBytes(ENCODING);
                } finally {
                    close(rs);
                }
            } else {
                return super.execute(conn);
            }

        } finally {
            close(stmt);
        }
    }

    private int[] setArguments(PreparedStatement pstmt) throws SQLException, IOException {
        if (getQueryArguments().trim().length() == 0) {
            return new int[]{};
        }
        String[] arguments = CSVSaveService.csvSplitString(getQueryArguments(), COMMA_CHAR);
        String[] argumentsTypes = getQueryArgumentsTypes().split(COMMA);
        if (arguments.length != argumentsTypes.length) {
            throw new SQLException("number of arguments (" + arguments.length + ") and number of types (" + argumentsTypes.length + ") are not equal");
        }
        int[] outputs = new int[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            String argument = arguments[i];
            String argumentType = argumentsTypes[i];
            String[] arg = argumentType.split(" ");
            String inputOutput = "";
            if (arg.length > 1) {
                argumentType = arg[1];
                inputOutput = arg[0];
            }
            int targetSqlType = getJdbcType(argumentType);
            try {
                if (!OUT.equalsIgnoreCase(inputOutput)) {
                    if (argument.equals(NULL_MARKER)) {
                        pstmt.setNull(i + 1, targetSqlType);
                    } else {
                        pstmt.setObject(i + 1, argument, targetSqlType);
                    }
                }
                if (OUT.equalsIgnoreCase(inputOutput) || INOUT.equalsIgnoreCase(inputOutput)) {
                    CallableStatement cs = (CallableStatement) pstmt;
                    cs.registerOutParameter(i + 1, targetSqlType);
                    outputs[i] = targetSqlType;
                } else {
                    outputs[i] = java.sql.Types.NULL; // can't have an output parameter type null
                }
            } catch (NullPointerException e) { // thrown by Derby JDBC (at least) if there are no "?" markers in statement
                throw new SQLException("Could not set argument no: " + (i + 1) + " - missing parameter marker?");
            }
        }
        return outputs;
    }

    private CallableStatement getCallableStatement(Connection conn) throws SQLException {
        return (CallableStatement) getPreparedStatement(conn, true);

    }

    private PreparedStatement getPreparedStatement(Connection conn) throws SQLException {
        return getPreparedStatement(conn, false);
    }

    private PreparedStatement getPreparedStatement(Connection conn, boolean callable) throws SQLException {
        Map<String, PreparedStatement> preparedStatementMap = perConnCache.get(conn);
        if (null == preparedStatementMap) {
            @SuppressWarnings("unchecked") // LRUMap is not generic
                    Map<String, PreparedStatement> lruMap = new LRUMap(MAX_OPEN_PREPARED_STATEMENTS) {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeLRU(AbstractLinkedMap.LinkEntry entry) {
                    PreparedStatement preparedStatement = (PreparedStatement) entry.getValue();
                    close(preparedStatement);
                    return true;
                }
            };
            preparedStatementMap = Collections.<String, PreparedStatement>synchronizedMap(lruMap);
            // As a connection is held by only one thread, we cannot already have a
            // preparedStatementMap put by another thread
            perConnCache.put(conn, preparedStatementMap);
        }
        PreparedStatement pstmt = preparedStatementMap.get(getQuery());
        if (null == pstmt) {
            if (callable) {
                pstmt = conn.prepareCall(getQuery());
            } else {
                pstmt = conn.prepareStatement(getQuery());
            }
            pstmt.setQueryTimeout(getIntegerQueryTimeout());
            // PreparedStatementMap is associated to one connection so
            //  2 threads cannot use the same PreparedStatement map at the same time
            preparedStatementMap.put(getQuery(), pstmt);
        } else {
            int timeoutInS = getIntegerQueryTimeout();
            if (pstmt.getQueryTimeout() != timeoutInS) {
                pstmt.setQueryTimeout(getIntegerQueryTimeout());
            }
        }
        pstmt.clearParameters();
        return pstmt;
    }

    private String resultSetsToString(PreparedStatement pstmt, boolean result, int[] out) throws SQLException, UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        int updateCount = 0;
        if (!result) {
            updateCount = pstmt.getUpdateCount();
        }
        do {
            if (result) {
                ResultSet rs = null;
                try {
                    rs = pstmt.getResultSet();
                    sb.append(getStringFromResultSet(rs)).append("\n"); // $NON-NLS-1$
                } finally {
                    close(rs);
                }
            } else {
                sb.append(updateCount).append(" updates.\n");
            }
            result = pstmt.getMoreResults();
            if (!result) {
                updateCount = pstmt.getUpdateCount();
            }
        } while (result || (updateCount != -1));
        if (out != null && pstmt instanceof CallableStatement) {
            ArrayList<Object> outputValues = new ArrayList<Object>();
            CallableStatement cs = (CallableStatement) pstmt;
            sb.append("Output variables by position:\n");
            for (int i = 0; i < out.length; i++) {
                if (out[i] != java.sql.Types.NULL) {
                    Object o = cs.getObject(i + 1);
                    outputValues.add(o);
                    sb.append("[");
                    sb.append(i + 1);
                    sb.append("] ");
                    sb.append(o);
                    if (o instanceof java.sql.ResultSet && RS_COUNT_RECORDS.equals(resultSetHandler)) {
                        sb.append(" ").append(countRows((ResultSet) o)).append(" rows");
                    }
                    sb.append("\n");
                }
            }
            String varnames[] = getVariableNames().split(COMMA);
            if (varnames.length > 0) {
                JMeterVariables jmvars = getThreadContext().getVariables();
                for (int i = 0; i < varnames.length && i < outputValues.size(); i++) {
                    String name = varnames[i].trim();
                    if (name.length() > 0) { // Save the value in the variable if present
                        Object o = outputValues.get(i);
                        if (o instanceof java.sql.ResultSet) {
                            ResultSet resultSet = (ResultSet) o;
                            if (RS_STORE_AS_OBJECT.equals(resultSetHandler)) {
                                jmvars.putObject(name, o);
                            } else if (RS_COUNT_RECORDS.equals(resultSetHandler)) {
                                jmvars.put(name, o.toString() + " " + countRows(resultSet) + " rows");
                            } else {
                                jmvars.put(name, o.toString());
                            }
                        } else {
                            jmvars.put(name, o == null ? null : o.toString());
                        }
                    }
                }
            }
        }
        return sb.toString();
    }

    private static int getJdbcType(String jdbcType) throws SQLException {
        Integer entry = mapJdbcNameToInt.get(jdbcType.toLowerCase(java.util.Locale.ENGLISH));
        if (entry == null) {
            try {
                entry = Integer.decode(jdbcType);
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid data type: " + jdbcType);
            }
        }
        return (entry).intValue();
    }

    /**
     * Count rows in result set
     *
     * @param resultSet {@link ResultSet}
     * @return number of rows in resultSet
     * @throws SQLException
     */
    private static final int countRows(ResultSet resultSet) throws SQLException {
        return resultSet.last() ? resultSet.getRow() : 0;
    }

    /**
     * Gets a Data object from a ResultSet.
     *
     * @param rs ResultSet passed in from a database query
     * @return a Data object
     * @throws java.sql.SQLException
     * @throws UnsupportedEncodingException
     */
    private String getStringFromResultSet(ResultSet rs) throws SQLException, UnsupportedEncodingException {
        ResultSetMetaData meta = rs.getMetaData();

        StringBuilder sb = new StringBuilder();

        int numColumns = meta.getColumnCount();
        for (int i = 1; i <= numColumns; i++) {
            sb.append(meta.getColumnLabel(i));
            if (i == numColumns) {
                sb.append('\n');
            } else {
                sb.append('\t');
            }
        }


        JMeterVariables jmvars = getThreadContext().getVariables();
        String varnames[] = getVariableNames().split(COMMA);
        String resultVariable = getResultVariable().trim();
        List<Map<String, Object>> results = null;
        if (resultVariable.length() > 0) {
            results = new ArrayList<Map<String, Object>>();
            jmvars.putObject(resultVariable, results);
        }
        int j = 0;
        while (rs.next()) {
            Map<String, Object> row = null;
            j++;
            for (int i = 1; i <= numColumns; i++) {
                Object o = rs.getObject(i);
                if (results != null) {
                    if (row == null) {
                        row = new HashMap<String, Object>(numColumns);
                        results.add(row);
                    }
                    row.put(meta.getColumnLabel(i), o);
                }
                if (o instanceof byte[]) {
                    o = new String((byte[]) o, ENCODING);
                }
                sb.append(o);
                if (i == numColumns) {
                    sb.append('\n');
                } else {
                    sb.append('\t');
                }
                if (i <= varnames.length) { // i starts at 1
                    String name = varnames[i - 1].trim();
                    if (name.length() > 0) { // Save the value in the variable if present
                        jmvars.put(name + UNDERSCORE + j, o == null ? null : o.toString());
                    }
                }
            }
            if (NumberUtils.isNumber(getResultSetReadDelay())) {
                try {
                    Thread.sleep(Long.valueOf(getResultSetReadDelay()));
                } catch (InterruptedException e) {
                    log.warn("Result set read delay caused InterruptedException.", e);
                }
            }
        }
        // Remove any additional values from previous sample
        for (int i = 0; i < varnames.length; i++) {
            String name = varnames[i].trim();
            if (name.length() > 0 && jmvars != null) {
                final String varCount = name + "_#"; // $NON-NLS-1$
                // Get the previous count
                String prevCount = jmvars.get(varCount);
                if (prevCount != null) {
                    int prev = Integer.parseInt(prevCount);
                    for (int n = j + 1; n <= prev; n++) {
                        jmvars.remove(name + UNDERSCORE + n);
                    }
                }
                jmvars.put(varCount, Integer.toString(j)); // save the current count
            }
        }

        return sb.toString();
    }

    public String getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(String fetchSize) {
        this.fetchSize = fetchSize;
    }

    public String getResultSetReadDelay() {
        return resultSetReadDelay;
    }

    public void setResultSetReadDelay(String resultSetReadDelay) {
        this.resultSetReadDelay = resultSetReadDelay;
    }
}
