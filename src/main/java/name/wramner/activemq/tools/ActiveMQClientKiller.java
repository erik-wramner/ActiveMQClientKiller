/*
 * Copyright 2016 Erik Wramner.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package name.wramner.activemq.tools;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.sun.tools.attach.VirtualMachine;

/**
 * Utility that stops connections for specific ActiveMQ clients identified by IP. This can be useful
 * for ActiveMQ clusters without dynamic load balancing where performance suffers if too many
 * clients are connected to a specific broker. Provided that the clients are smart enough to
 * reconnect (which is supported out of the box with the fail-over URL or the resource adapter)
 * this will rebalance the cluster without having to resort to iptables.
 *
 * @author Erik Wramner
 */
public class ActiveMQClientKiller {
    private static final String CLIENT_CONNECTOR_OBJECT_NAME_PATTERN = "org.apache.activemq:type=Broker,connector=clientConnectors,connectionViewType=remoteAddress,*";

    /**
     * Program entry point.
     *
     * @param args The arguments.
     */
    public static void main(String[] args) {
        Config config = new Config();
        if (parseCommandLine(args, config)) {
            try {
                VirtualMachine virtualMachine = VirtualMachine.attach(String.valueOf(config.getProcessId()));
                try {
                    String connectorAddress = virtualMachine.getAgentProperties()
                        .getProperty("com.sun.management.jmxremote.localConnectorAddress");
                    if (connectorAddress != null) {
                        int stoppedConnections = stopClientConnections(connectToMBeanServer(connectorAddress),
                            config.getClientIp(), config.getMaxConnections());

                        if (stoppedConnections > 0) {
                            System.out.println("Stopped " + stoppedConnections + " client connections");
                        }
                        else {
                            System.out.println("No matching client connections");
                        }
                    }
                    else {
                        System.err.println("Could not find local JMX connector address");
                    }
                }
                finally {
                    virtualMachine.detach();
                }
            }
            catch (Exception e) {
                System.err.println("Failed to stop client connections!");
                e.printStackTrace(System.err);
            }
        }
    }

    private static int stopClientConnections(MBeanServerConnection conn, String clientIp, int maxConnectionsToStop)
            throws IOException, MalformedObjectNameException, InstanceNotFoundException, MBeanException,
            ReflectionException {
        String delimitedClientIp = "//" + clientIp + "_";
        int stoppedConnections = 0;
        for (ObjectInstance oi : findConnectionViewMBeans(conn)) {
            if (oi.getClassName().equals("org.apache.activemq.broker.jmx.ConnectionView")) {
                String connectionName = oi.getObjectName().getKeyProperty("connectionName");
                if (connectionName != null && connectionName.contains(delimitedClientIp)) {
                    conn.invoke(oi.getObjectName(), "stop", new Object[0], new String[0]);
                    if (++stoppedConnections >= maxConnectionsToStop) {
                        break;
                    }
                }
            }
        }
        return stoppedConnections;
    }

    private static MBeanServerConnection connectToMBeanServer(String connectorAddress)
            throws IOException, MalformedURLException {
        return JMXConnectorFactory.connect(new JMXServiceURL(connectorAddress)).getMBeanServerConnection();
    }

    private static Set<ObjectInstance> findConnectionViewMBeans(MBeanServerConnection conn)
            throws IOException, MalformedObjectNameException {
        return conn.queryMBeans(new ObjectName(CLIENT_CONNECTOR_OBJECT_NAME_PATTERN), null);
    }

    private static boolean parseCommandLine(String[] args, Config config) {
        CmdLineParser parser = new CmdLineParser(config);
        try {
            parser.parseArgument(args);
            return true;
        }
        catch (CmdLineException e) {
            System.out.println("Error: " + e.getMessage());
            System.out.println("The supported options are:");
            parser.printUsage(System.out);
            System.out.println();
            return false;
        }
    }

    private static class Config {
        @Option(name = "-a", aliases = { "--ip-address" }, usage = "IP address for client to kill", required = true)
        private String _clientIp;

        @Option(name = "-p", aliases = { "--pid" }, usage = "ActiveMQ process id", required = true)
        private int _processId;

        @Option(name = "-c", aliases = {
                "--max-connections-to-stop" }, usage = "Maximum number of client connections to stop", required = false)
        private Integer _maxConnections;

        public String getClientIp() {
            return _clientIp;
        }

        public int getProcessId() {
            return _processId;
        }

        public int getMaxConnections() {
            return _maxConnections != null ? _maxConnections.intValue() : Integer.MAX_VALUE;
        }
    }
}
