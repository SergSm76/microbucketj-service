package my.app.microbucketj.utils;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import my.app.microbucketj.config.SslConfig;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

public final class TlsSupport {

  private TlsSupport() {
  }

  public static SslContext buildServerMtlsContext(
      SslConfig sslConfig
  ) throws Exception {

    KeyManagerFactory kmf = buildKeyManagerFactory(
        sslConfig.tlsKeyStoreFile(), sslConfig.tlsKeyStorePassword(), sslConfig.tlsKeyPassword(), sslConfig.tlsKeyStoreType()
    );
    TrustManagerFactory tmf = buildTrustManagerFactory(
        sslConfig.tlsTrustStoreFile(), sslConfig.tlsTrustStorePassword(), sslConfig.tlsTrustStoreType()
    );

    return SslContextBuilder
        .forServer(kmf)
        .trustManager(tmf)
        .clientAuth(ClientAuth.REQUIRE)
        .build();
  }

  public static SslContext buildClientMtlsContext(
      String keyStoreFile,
      String keyStorePassword,
      String keyPassword,
      String keyStoreType,
      String trustStoreFile,
      String trustStorePassword,
      String trustStoreType
  ) throws Exception {

    KeyManagerFactory kmf = buildKeyManagerFactory(
        keyStoreFile, keyStorePassword, keyPassword, keyStoreType
    );
    TrustManagerFactory tmf = buildTrustManagerFactory(
        trustStoreFile, trustStorePassword, trustStoreType
    );

    return SslContextBuilder
        .forClient()
        .keyManager(kmf)
        .trustManager(tmf)
        .endpointIdentificationAlgorithm("HTTPS")
        .build();
  }

  private static KeyManagerFactory buildKeyManagerFactory(
      String keyStoreFile,
      String keyStorePassword,
      String keyPassword,
      String keyStoreType
  ) throws Exception {
    KeyStore ks = KeyStore.getInstance(emptyToDefault(keyStoreType, "PKCS12"));
    char[] ksPass = toPassword(keyStorePassword);

    try (InputStream in = new FileInputStream(keyStoreFile)) {
      ks.load(in, ksPass);
    }

    KeyManagerFactory kmf =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, toPassword(keyPassword != null && !keyPassword.isBlank()
        ? keyPassword
        : keyStorePassword));
    return kmf;
  }

  private static TrustManagerFactory buildTrustManagerFactory(
      String trustStoreFile,
      String trustStorePassword,
      String trustStoreType
  ) throws Exception {
    KeyStore ts = KeyStore.getInstance(emptyToDefault(trustStoreType, "PKCS12"));
    char[] tsPass = toPassword(trustStorePassword);

    try (InputStream in = new FileInputStream(trustStoreFile)) {
      ts.load(in, tsPass);
    }

    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    return tmf;
  }

  private static char[] toPassword(String value) {
    return value == null ? null : value.toCharArray();
  }

  private static String emptyToDefault(String value, String def) {
    return (value == null || value.isBlank()) ? def : value;
  }
}