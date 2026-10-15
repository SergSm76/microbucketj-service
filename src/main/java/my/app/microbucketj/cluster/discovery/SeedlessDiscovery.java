package my.app.microbucketj.cluster.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.cluster.model.HostAndPort;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record SeedlessDiscovery(
    String dns,
    HostAndPort[] staticHosts,
    int clusterPort
) {

  private static final Logger LOG = LoggerFactory.getLogger(SeedlessDiscovery.class);

  public SeedlessDiscovery(String dns, HostAndPort[] staticHosts, int clusterPort) {
    this.dns = dns == null ? "" : dns.trim();
    this.staticHosts = staticHosts == null ? new HostAndPort[0] : staticHosts;
    this.clusterPort = clusterPort;
  }

  public List<HostAndPort> candidates(String selfHost, int clusterPort) {
    Set<HostAndPort> out = new LinkedHashSet<>();

    for (HostAndPort h : staticHosts) {
      if (h != null) {
        out.add(h);
      }
    }

    if (!dns.isEmpty()) {
      try {
        InetAddress[] all = InetAddress.getAllByName(dns);
        for (InetAddress a : all) {
          if (a == null) {
            continue;
          }
          String ip = a.getHostAddress();
          if (ip != null && !ip.isBlank()) {
            out.add(new HostAndPort(ip, this.clusterPort));
          }
        }
      } catch (Exception exception) {
        LOG.error("Не удалось разрешить DNS-имя: {}", dns, exception);
      }
    }

    // exclude self
    if (selfHost != null && !selfHost.isBlank()) {
      out.remove(new HostAndPort(selfHost, clusterPort));
    }

    List<HostAndPort> res = new ArrayList<>(out);
    LOG.debug("Кандидаты discovery: DNS='{}' static={} result={} (self={})", dns, staticHosts.length, res.size(), selfHost);
    return res;
  }
}
