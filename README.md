# jmeter-jdbc-sampler-advanced
JMeter Sampler with ability to control certain parameters of the java.sql.Statement and java.sql.ResultSet

The plugin extends standard JDBC sampler, see: http://jmeter.apache.org/usermanual/component_reference.html#JDBC_Request so the configuration of the sampler is the same + additional options:

* For the java.sql.Statement fetch size can be set to a desired value. If not set it will use the default value of 10
* Result set read delay option is meant to simulate slow processing client and should be set in milliseconds.
