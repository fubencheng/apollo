// 本地模式
storm jar /Users/ben/java_workspace/storm-starter/target/storm-starter-1.0-SNAPSHOT.jar com.netlink.WordCountTopology

// 集群模式
storm jar /Users/ben/java_workspace/storm-starter/target/storm-starter-1.0-SNAPSHOT.jar com.netlink.WordCountTopology world-count-topology

// 只有在集群模式下提交的拓扑才能在Storm UI Topology Summary看到

// 停止拓扑
storm kill world-count-topology