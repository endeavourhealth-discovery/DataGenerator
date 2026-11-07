package org.endeavourhealth.cegdatabasefilesender;

import java.util.UUID;

public class Payload {

    private UUID queuedMessageId;
    private long filingOrder;
    private byte[] queuedMessageBody;

    public Payload() {
    }

    public UUID getQueuedMessageId() {
        return queuedMessageId;
    }

    public void setQueuedMessageId(UUID queuedMessageId) {
        this.queuedMessageId = queuedMessageId;
    }

    public long getFilingOrder() {
        return filingOrder;
    }

    public void setFilingOrder(long filingOrder) {
        this.filingOrder = filingOrder;
    }

    public byte[] getQueuedMessageBody() {
        return queuedMessageBody;
    }

    public void setQueuedMessageBody(byte[] queuedMessageBody) {
        this.queuedMessageBody = queuedMessageBody;
    }

}
