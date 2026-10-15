package my.app.microbucketj;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import my.app.microbucketj.bootstrap.MicroBucketBootstrap;
import my.app.microbucketj.bootstrap.RuntimeComponents;
import my.app.microbucketj.config.Config;

public final class MicroBucketJServerMain {

  private static final Logger LOG = LoggerFactory.getLogger(MicroBucketJServerMain.class);

  public static void main(String[] args) throws Exception {
    Config cfg = Config.fromEnv();
    MDC.put("nodeId", cfg.nodeId);

    RuntimeComponents rt = MicroBucketBootstrap.bootstrap(LOG, cfg);
    cfg.logSummary();

    Runtime.getRuntime().addShutdownHook(new Thread(rt::close, "shutdown"));
  }
}
