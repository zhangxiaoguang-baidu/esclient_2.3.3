# esclient_2.3.3
对es2.3.3原生api的封装
# 接入
···
<bean class="com.meituan.trip.eswrapper.es.connection.ESConnection" id="esConnections">
    <constructor-arg name="transportAddresses">
        <list>
            <ref bean="staging01"/>
            <ref bean="staging02"/>
        </list>
    </constructor-arg>
    <constructor-arg name="clusterName" value="trip.c.es.stage"/>
</bean>
<bean id="inetSocketAddress1" class="java.net.InetSocketAddress">
    <constructor-arg index="0" value="es server ip地址"/>
    <constructor-arg index="1" value="es server 端口号"/>
</bean>
<bean id="inetSocketAddress2" class="java.net.InetSocketAddress">
    <constructor-arg index="0" value="es server ip地址"/>
    <constructor-arg index="1" value="es server 端口号"/>
</bean>
<bean id="staging01" class="org.elasticsearch.common.transport.InetSocketTransportAddress">
    <constructor-arg name="address" ref="inetSocketAddress1"/>
</bean>
<bean id="staging02" class="org.elasticsearch.common.transport.InetSocketTransportAddress">
    <constructor-arg name="address" ref="inetSocketAddress2"/>
</bean>
 
<!--最终对外暴露该API即可-->
<bean class="com.meituan.trip.eswrapper.es.operation.PlentyfulESOperationImpl" id="esOperations">
    <property name="esConnections" ref="esConnections"/>
</bean>
···
