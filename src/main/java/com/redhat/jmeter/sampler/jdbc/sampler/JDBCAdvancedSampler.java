package com.redhat.jmeter.sampler.jdbc.sampler;

import com.redhat.jmeter.sampler.jdbc.AbstractJDBCAdvancedTestElement;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.engine.util.ConfigMergabilityIndicator;
import org.apache.jmeter.protocol.jdbc.config.DataSourceElement;
import org.apache.jmeter.protocol.jdbc.sampler.JDBCSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.Sampler;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.util.JOrphanUtils;
import org.apache.log.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by vklevko on 2/29/16.
 */
public class JDBCAdvancedSampler extends AbstractJDBCAdvancedTestElement implements Sampler, TestBean, ConfigMergabilityIndicator {

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final Set<String> APPLIABLE_CONFIG_CLASSES = new HashSet<String>(
            Arrays.asList(new String[]{
                    "org.apache.jmeter.config.gui.SimpleConfigGui"}));



    public SampleResult sample(Entry entry) {
        log.debug("sampling jdbc");

        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());
        res.setSamplerData(toString());
        res.setDataType(SampleResult.TEXT);
        res.setContentType("text/plain"); // $NON-NLS-1$
        res.setDataEncoding(ENCODING);

        // Assume we will be successful
        res.setSuccessful(true);
        res.setResponseMessageOK();
        res.setResponseCodeOK();


        res.sampleStart();
        Connection conn = null;

        try {
            if(JOrphanUtils.isBlank(getDataSource())) {
                throw new IllegalArgumentException("Variable Name must not be null in "+getName());
            }

            try {
                conn = DataSourceElement.getConnection(getDataSource());
            } finally {
                // FIXME: there is separate connect time field now
                res.latencyEnd(); // use latency to measure connection time
            }
            res.setResponseHeaders(conn.toString());
            res.setResponseData(execute(conn));
        } catch (SQLException ex) {
            final String errCode = Integer.toString(ex.getErrorCode());
            res.setResponseMessage(ex.toString());
            res.setResponseCode(ex.getSQLState()+ " " +errCode);
            res.setResponseData(ex.getMessage().getBytes());
            res.setSuccessful(false);
        } catch (Exception ex) {
            res.setResponseMessage(ex.toString());
            res.setResponseCode("000");
            res.setResponseData(ex.getMessage().getBytes());
            res.setSuccessful(false);
        } finally {
            close(conn);
        }

        // TODO: process warnings? Set Code and Message to success?
        res.sampleEnd();
        return res;
    }

    public boolean applies(ConfigTestElement configTestElement) {
        String guiClass = configTestElement.getProperty(TestElement.GUI_CLASS).getStringValue();
        return APPLIABLE_CONFIG_CLASSES.contains(guiClass);
    }
}
