package org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition;

public class SftpConnectionDetails {
    private String hostname = null;
    private String hostPublicKey = null;
    private int port;
    private String username = null;
    private String clientPrivateKeyPassword = null;
    private String clientPrivateKey = null;

    /* private SftpConnectionDetails() {}

    private SftpConnectionDetails(String hostname, String hostPublicKey,
                                  Integer port, String username,
                                  String clientPrivateKeyPassword,
                                  String clientPrivateKey) {
        this.hostname = hostname;
        this.hostPublicKey = hostPublicKey;
        this.port = port;
        this.username = username;
        this.clientPrivateKeyPassword = clientPrivateKeyPassword;
        this.clientPrivateKey = clientPrivateKey;
    } */

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getHostPublicKey() {
        return hostPublicKey;
    }

    public void setHostPublicKey(String hostPublicKey) {
        this.hostPublicKey = hostPublicKey;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getClientPrivateKeyPassword() {
        return clientPrivateKeyPassword;
    }

    public void setClientPrivateKeyPassword(String clientPrivateKeyPassword) {
        this.clientPrivateKeyPassword = clientPrivateKeyPassword;
    }

    public String getClientPrivateKey() {
        return clientPrivateKey;
    }

    public void setClientPrivateKey(String clientPrivateKey) {
        this.clientPrivateKey = clientPrivateKey;
    }
}