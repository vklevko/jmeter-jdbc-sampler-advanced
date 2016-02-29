package com.redhat.jmeter.sampler.jdbc.sampler;

import com.redhat.jmeter.sampler.jdbc.JDBCAdvancedTestElementBeanInfoSupport;
import org.apache.jmeter.protocol.jdbc.JDBCTestElementBeanInfoSupport;
import org.apache.jmeter.testbeans.TestBean;

import java.beans.PropertyDescriptor;

/**
 * Created by vklevko on 2/29/16.
 */
public class JDBCAdvancedSamplerBeanInfo extends JDBCAdvancedTestElementBeanInfoSupport {
    public JDBCAdvancedSamplerBeanInfo() {
        super(JDBCAdvancedSampler.class);
    }
}
