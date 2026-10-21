package my.app.microbucketj.observability;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import my.app.microbucketj.api.model.LockEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.List;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ControlHttpServer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ControlHttpServer.class);
  private static final byte[] OK = "OK".getBytes(UTF_8);

  private final HttpServer server;

  public ControlHttpServer(String bindHost, int port, PrometheusMetrics metrics,
                           LockRegistryRenderer lockRegistryRenderer, boolean locksApiEnabled) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(bindHost, port), 64);
    this.server.createContext("/v1/actuator/health", new FixedHandler(200, "text/plain; charset=utf-8", OK));
    this.server.createContext("/v1/actuator/prometheus", new MetricsHandler(metrics));

    if (locksApiEnabled && lockRegistryRenderer != null) {
      this.server.createContext("/v1/locks", new LockRegistryHandler(lockRegistryRenderer));
      LOG.info("Locks Observability API включен по адресу /v1/locks");
    } else {
      LOG.info("Locks Observability API выключен");
    }

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
      byte[] body = metrics.render().getBytes(UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static final class LockRegistryHandler implements HttpHandler {
    private final LockRegistryRenderer renderer;

    private LockRegistryHandler(LockRegistryRenderer renderer) {
      this.renderer = renderer;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        // Парсим query параметр name для поиска (LIKE '%value%')
        String query = exchange.getRequestURI().getQuery();
        String nameFilter = null;
        if (query != null && !query.isBlank()) {
          for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && kv[0].equalsIgnoreCase("name")) {
              nameFilter = URLDecoder.decode(kv[1], UTF_8);
              break;
            }
          }
        }

        List<LockEntry> snapshot = renderer.getFilteredSnapshot(nameFilter);

        // Определяем формат ответа по заголовку Accept
        String accept = exchange.getRequestHeaders().getFirst("Accept");
        boolean isJson = accept != null && accept.toLowerCase().contains("application/json");

        byte[] body;
        String contentType;
        if (isJson) {
          contentType = "application/json; charset=utf-8";
          body = renderer.renderJson(snapshot).getBytes(UTF_8);
        } else {
          contentType = "text/plain; charset=utf-8";
          body = renderer.renderText(snapshot).getBytes(UTF_8);
        }

        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      } catch (Exception e) {
        LOG.error("LockRegistryHandler error", e);
        byte[] err = "Internal Server Error".getBytes(UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(500, err.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(err);
        }
      }
    }
  }
}
