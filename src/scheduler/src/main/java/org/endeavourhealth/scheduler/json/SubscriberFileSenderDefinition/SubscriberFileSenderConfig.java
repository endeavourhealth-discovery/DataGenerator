package org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition;

import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SftpConnectionDetails;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileLocationDetails;

public class SubscriberFileSenderConfig {

    private Integer id = null;
    private SubscriberFileLocationDetails subscriberFileLocationDetails;
    private SftpConnectionDetails sftpConnectionDetails;

    private SubscriberFileSenderConfig() {}

    private SubscriberFileSenderConfig(Integer id,
                                       SubscriberFileLocationDetails subscriberFileLocationDetails,
                                       SftpConnectionDetails sftpConnectionDetails) {
        this.id = id;
        this.subscriberFileLocationDetails = subscriberFileLocationDetails;
        this.sftpConnectionDetails = sftpConnectionDetails;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

}
