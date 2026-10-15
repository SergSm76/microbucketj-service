package my.app.microbucketj.observability;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

public final class ControlHttpServer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ControlHttpServer.class);
  private static final byte[] OK = "OK".getBytes(StandardCharsets.UTF_8);

  private final HttpServer server;

  public ControlHttpServer(String bindHost, int port, PrometheusMetrics metrics) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(bindHost, port), 64);
    this.server.createContext("/v1/actuator/health", new FixedHandler(200, "text/plain; charset=utf-8", OK));
    this.server.createContext("/v1/actuator/prometheus", new MetricsHandler(metrics));
    this.server.createContext("/", new FixedHandler(404, "text/plain; charset=utf-8", new byte[0]));
    this.server.setExecutor(Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "mbj-observability-http");
      t.setDaemon(true);
      return t;
    }));
    this.server.start();
    LOG.info("observability http привязан к {}:{}", bindHost, port);
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private record FixedHandler(int status, String contentType, byte[] body) implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      exchange.getResponseHeaders().set("Content-Type", contentType);
      exchange.sendResponseHeaders(status, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static final class MetricsHandler implements HttpHandler {
    private final PrometheusMetrics metrics;

    private MetricsHandler(PrometheusMetrics metrics) {
      this.metrics = metrics;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      byte[] body = metrics.render().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }
}
