package my.app.microbucketj.config;

public record SslConfig(
    boolean tlsEnabled,
    String tlsKeyStoreFile,
    String tlsKeyStorePassword,
    String tlsKeyPassword,
    String tlsKeyStoreType,
    String tlsTrustStoreFile,
    String tlsTrustStorePassword,
    String tlsTrustStoreType
) {
  public static SslConfig fromEnv() {

    boolean tlsEnabled = Env.getEnvBoolean("MTLS_ENABLED", false);

    String tlsKeyStoreFile = Env.getEnv("KEYSTORE_LOCATION", "");
    String tlsKeyStorePassword = Env.getEnv("KEYSTORE_PASSWORD", "");
    String tlsKeyPassword = Env.getEnv("KEYSTORE_PASSWORD", "");
    String tlsTrustStoreFile = Env.getEnv("TRUSTSTORE_LOCATION", "");
    String tlsTrustStorePassword = Env.getEnv("TRUSTSTORE_PASSWORD", "");

    String tlsKeyStoreType = Env.getEnv("KEYSTORE_TYPE", "PKCS12");
    String tlsTrustStoreType = Env.getEnv("TRUSTSTORE_TYPE", "PKCS12");

    return new SslConfig(
        tlsEnabled,
        tlsKeyStoreFile,
        tlsKeyStorePassword,
        tlsKeyPassword,
        tlsKeyStoreType,
        tlsTrustStoreFile,
        tlsTrustStorePassword,
        tlsTrustStoreType
    );
  }
}
