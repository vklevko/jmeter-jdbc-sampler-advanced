package com.redhat.jmeter.sampler.jdbc;

import org.apache.jmeter.protocol.jdbc.JDBCTestElementBeanInfoSupport;
import org.apache.jmeter.testbeans.TestBean;

import java.beans.PropertyDescriptor;

/**
 * Created by vklevko on 2/29/16.
 */
public class JDBCAdvancedTestElementBeanInfoSupport extends JDBCTestElementBeanInfoSupport {
    public JDBCAdvancedTestElementBeanInfoSupport(Class<? extends TestBean> beanClass) {
        super(beanClass);
        createPropertyGroup("statementConfig",
                // $NON-NLS-1$
                new String[]{
                "fetchSize" // $NON-NLS-1$
        });
        PropertyDescriptor p = property("fetchSize"); // $NON-NLS-1$
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);  // $NON-NLS-1$
        p.setValue(DEFAULT, 10);
        p.setValue(TEXT_LANGUAGE,"fetchSize");// $NON-NLS-1$

    }
}
