package com.redhat.jmeter.sampler.jdbc;

import org.apache.jmeter.protocol.jdbc.AbstractJDBCTestElement;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by vklevko on 2/29/16.
 */
public abstract class AbstractJDBCAdvancedTestElement extends AbstractJDBCTestElement {

    private static final Logger log = LoggingManager.getLoggerForClass();

    private int fetchSize=10;

    static final String SELECT = "Select Statement";
    private static final String COMMA = ","; // $NON-NLS-1$
    private static final String UNDERSCORE = "_"; // $NON-NLS-1$

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
                stmt.setFetchSize(getFetchSize());
                ResultSet rs = null;
                try {
                    rs = stmt.executeQuery(getQuery());
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

    /**
     * Gets a Data object from a ResultSet.
     *
     * @param rs
     *            ResultSet passed in from a database query
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
            if (i==numColumns){
                sb.append('\n');
            } else {
                sb.append('\t');
            }
        }


        JMeterVariables jmvars = getThreadContext().getVariables();
        String varnames[] = getVariableNames().split(COMMA);
        String resultVariable = getResultVariable().trim();
        List<Map<String, Object> > results = null;
        if(resultVariable.length() > 0) {
            results = new ArrayList<Map<String,Object> >();
            jmvars.putObject(resultVariable, results);
        }
        int j = 0;
        while (rs.next()) {
            Map<String, Object> row = null;
            j++;
            for (int i = 1; i <= numColumns; i++) {
                Object o = rs.getObject(i);
                if(results != null) {
                    if(row == null) {
                        row = new HashMap<String, Object>(numColumns);
                        results.add(row);
                    }
                    row.put(meta.getColumnLabel(i), o);
                }
                if (o instanceof byte[]) {
                    o = new String((byte[]) o, ENCODING);
                }
                sb.append(o);
                if (i==numColumns){
                    sb.append('\n');
                } else {
                    sb.append('\t');
                }
                if (i <= varnames.length) { // i starts at 1
                    String name = varnames[i - 1].trim();
                    if (name.length()>0){ // Save the value in the variable if present
                        jmvars.put(name+UNDERSCORE+j, o == null ? null : o.toString());
                    }
                }
            }
        }
        // Remove any additional values from previous sample
        for(int i=0; i < varnames.length; i++){
            String name = varnames[i].trim();
            if (name.length()>0 && jmvars != null){
                final String varCount = name+"_#"; // $NON-NLS-1$
                // Get the previous count
                String prevCount = jmvars.get(varCount);
                if (prevCount != null){
                    int prev = Integer.parseInt(prevCount);
                    for (int n=j+1; n <= prev; n++ ){
                        jmvars.remove(name+UNDERSCORE+n);
                    }
                }
                jmvars.put(varCount, Integer.toString(j)); // save the current count
            }
        }

        return sb.toString();
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }
}
