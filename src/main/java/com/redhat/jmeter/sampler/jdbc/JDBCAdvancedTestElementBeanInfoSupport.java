package com.redhat.jmeter.sampler.jdbc;

import org.apache.jmeter.protocol.jdbc.JDBCTestElementBeanInfoSupport;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testbeans.gui.BooleanPropertyEditor;

import java.beans.PropertyDescriptor;

/**
 * Created by vklevko on 2/29/16.
 */
public abstract class JDBCAdvancedTestElementBeanInfoSupport extends JDBCTestElementBeanInfoSupport {
    public JDBCAdvancedTestElementBeanInfoSupport(Class<? extends TestBean> beanClass) {
        super(beanClass);
        createPropertyGroup("statementConfig",
                // $NON-NLS-1$
                new String[]{
                        "fetchSize" // $NON-NLS-1$
                });

        createPropertyGroup("resultSetConfig", // $NON-NLS-1$
                new String[]{
                        "resultSetReadDelay" // $NON-NLS-1$
                });
        PropertyDescriptor p = property("fetchSize"); // $NON-NLS-1$
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);  // $NON-NLS-1$
        p.setValue(DEFAULT, 10);

        p = property("resultSetReadDelay"); // $NON-NLS-1$
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, 0);// $NON-NLS-1$
    }
}
