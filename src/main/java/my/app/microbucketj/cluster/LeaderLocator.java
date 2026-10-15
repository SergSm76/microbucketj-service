package my.app.microbucketj.cluster;

public interface LeaderLocator {

  boolean isLeader();

  String leaderId();
}
