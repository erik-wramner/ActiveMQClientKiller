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

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
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
    private static final String SUBSCRIBER_OBJECT_NAME_PATTERN_FMT = "org.apache.activemq:type=Broker,destinationName=%s,endpoint=Consumer,*";

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
                        int stoppedConnections = config.getDestinationName() != null
                                ? stopSubscriberConnections(connectToMBeanServer(connectorAddress),
                                    config.getDestinationName(), config.getMaxConnections(), config.isVerbose())
                                : stopClientConnections(connectToMBeanServer(connectorAddress), config.getClientIp(),
                                    config.getMaxConnections(), config.isVerbose());
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

    private static int stopClientConnections(MBeanServerConnection conn, String clientIp, int maxConnectionsToStop,
            boolean verbose) throws IOException, MalformedObjectNameException, InstanceNotFoundException,
            MBeanException, ReflectionException {
        String delimitedClientIp = "//" + clientIp + "_";
        int stoppedConnections = 0;
        if (verbose) {
            System.out.println("Finding ConnectionView MBeans...");
        }
        Set<ObjectInstance> mBeans = findConnectionViewMBeans(conn);
        if (verbose) {
            System.out.println("Processing " + mBeans.size() + " beans...");
        }
        for (ObjectInstance oi : mBeans) {
            if (oi.getClassName().equals("org.apache.activemq.broker.jmx.ConnectionView")) {
                String connectionName = oi.getObjectName().getKeyProperty("connectionName");
                if (connectionName != null && connectionName.contains(delimitedClientIp)) {
                    if (verbose) {
                        System.out.println("Stopping " + connectionName + "...");
                    }
                    try {
                        conn.invoke(oi.getObjectName(), "stop", new Object[0], new String[0]);
                        if (verbose) {
                            System.out.println("Stopped " + connectionName);
                        }
                    }
                    catch (OperationsException e) {
                        System.out.println("Failed to stop " + connectionName);
                    }
                    if (++stoppedConnections >= maxConnectionsToStop) {
                        break;
                    }
                }
            }
        }
        return stoppedConnections;
    }

    private static int stopSubscriberConnections(MBeanServerConnection conn, String destinationName,
            int maxConnectionsToStop, boolean verbose) throws IOException, MalformedObjectNameException,
            InstanceNotFoundException, MBeanException, ReflectionException {
        int stoppedConnections = 0;
        if (verbose) {
            System.out.println("Finding SubscriptionView MBeans for " + destinationName + "...");
        }
        Set<ObjectInstance> mBeans = findSubscriptionViewMBeans(conn, destinationName);
        if (verbose) {
            System.out.println("Processing " + mBeans.size() + " beans...");
        }
        for (ObjectInstance oi : mBeans) {
            if (oi.getClassName().equals("org.apache.activemq.broker.jmx.SubscriptionView")) {
                ObjectName connectionName = null;

                if (oi.getObjectName().getKeyProperty("consumerId").indexOf("->") == -1) {
                    try {
                        connectionName = (ObjectName) conn.getAttribute(oi.getObjectName(), "Connection");
                    }
                    catch (InstanceNotFoundException e) {
                        // Ignore, the connection is already closed
                    }
                    catch (AttributeNotFoundException e) {
                        System.out.println("Connection attribute missing for " + oi.getObjectName() + " - ignoring");
                    }
                }
                else if (verbose) {
                    System.out.println("Skipping bridge " + oi.getObjectName());
                }

                if (connectionName != null) {
                    if (verbose) {
                        System.out.println("Stopping " + connectionName + "...");
                    }
                    try {
                        conn.invoke(connectionName, "stop", new Object[0], new String[0]);
                        if (verbose) {
                            System.out.println("Stopped " + connectionName);
                        }
                        if (++stoppedConnections >= maxConnectionsToStop) {
                            break;
                        }
                    }
                    catch (OperationsException e) {
                        if (verbose) {
                            System.out.println("Failed to stop " + connectionName);
                        }
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

    private static Set<ObjectInstance> findSubscriptionViewMBeans(MBeanServerConnection conn, String destinationName)
            throws IOException, MalformedObjectNameException {
        return conn.queryMBeans(new ObjectName(String.format(SUBSCRIBER_OBJECT_NAME_PATTERN_FMT, destinationName)),
            null);
    }

    private static boolean parseCommandLine(String[] args, Config config) {
        CmdLineParser parser = new CmdLineParser(config);
        try {
            parser.parseArgument(args);
            if (config.getClientIp() != null || config.getDestinationName() != null) {
                return true;
            }
            else {
                System.out.println("Please specify IP address or destination name.");
            }
        }
        catch (CmdLineException e) {
            System.out.println("Error: " + e.getMessage());
        }
        printUsage(parser);
        return false;
    }

    private static void printUsage(CmdLineParser parser) {
        System.out.println("The supported options are:");
        parser.printUsage(System.out);
        System.out.println();
    }

    private static class Config {
        @Option(name = "-a", aliases = { "--ip-address" }, usage = "IP address for client to kill")
        private String _clientIp;

        @Option(name = "-d", aliases = { "--destination-name" }, usage = "Queue or topic name to kill clients for")
        private String _destinationName;

        @Option(name = "-p", aliases = { "--pid" }, usage = "ActiveMQ process id", required = true)
        private int _processId;

        @Option(name = "-c", aliases = {
                "--max-connections-to-stop" }, usage = "Maximum number of client connections to stop", required = false)
        private Integer _maxConnections;

        @Option(name = "-v", aliases = { "--verbose" }, usage = "Verbose logging")
        private boolean _verbose;

        public String getClientIp() {
            return _clientIp;
        }

        public int getProcessId() {
            return _processId;
        }

        public int getMaxConnections() {
            return _maxConnections != null ? _maxConnections.intValue() : Integer.MAX_VALUE;
        }

        public String getDestinationName() {
            return _destinationName;
        }

        public boolean isVerbose() {
            return _verbose;
        }
    }
}
