package org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition;

public class SubscriberFileSenderConfig {

    private int subscriberId;
    private SubscriberFileLocationDetails subscriberFileLocationDetails;
    private SftpConnectionDetails sftpConnectionDetails;
    //private String slackWebhook;

    private SubscriberFileSenderConfig() {}

    public int getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(int subscriberId) {
        this.subscriberId = subscriberId;
    }

    public SubscriberFileLocationDetails getSubscriberFileLocationDetails() {
        return subscriberFileLocationDetails;
    }

    public void setSubscriberFileLocationDetails(SubscriberFileLocationDetails fileLocationDetails) {
        this.subscriberFileLocationDetails = fileLocationDetails;
    }

    public SftpConnectionDetails getSftpConnectionDetails() {
        return sftpConnectionDetails;
    }

    public void setSftpConnectionDetails(SftpConnectionDetails sftpConnectionDetails) {
        this.sftpConnectionDetails = sftpConnectionDetails;
    }

    /*public String getSlackWebhook() {
        return slackWebhook;
    }

    public void setSlackWebhook(String slackWebhook) {
        this.slackWebhook = slackWebhook;
    }*/

}
