# ActiveMQClientKiller

This repository contains a command that kills (stops) ActiveMQ client
connections for a given remote IP using JMX commands to a local broker.

It connects to the local broker using the attach API, which uses classes
in tools.jar. Unfortunately the API uses internal Sun classes, so this only
works with Oracle's JVM and OpenJDK. IBM has a similar API, but with other
class names. In Java 9 this has been fixed, but the tool is written for
Java 8, so it will currently not work with Java 9.

Why kill client connections? ActiveMQ clusters without dynamic load
balancing can become unbalanced with many clients connected to a
single broker. Manually forcing the unbalanced clients to reconnect
re-balances the cluster, hopefully improving performance.

Run the command with (Windows syntax):

java -cp %JAVA_HOME%/lib/tools.jar;kill-activemq-clients.jar
  name.wramner.activemq.tools.ActiveMQClientKiller
  -a client-ip -p activemq-pid -c max-connections-to-stop

  