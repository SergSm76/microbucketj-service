package my.app.microbucketj.cluster.model;

public record Node(String id, String host, int clusterPort) {

  public String clusterAddrKey() {
    return host + ":" + clusterPort;
  }
}
