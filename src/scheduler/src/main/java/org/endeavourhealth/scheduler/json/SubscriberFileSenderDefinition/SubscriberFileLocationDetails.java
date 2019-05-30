package org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition;

public class SubscriberFileLocationDetails {

    private String dataDir = null;
    private String stagingDir = null;
    private String destinationDir = null;
    private String archiveDir = null;

    /* private SubcriberFileLocationDetails() {}

    private SubscriberFileLocationDetails(String dataDir, String stagingDir,
                                          String destinationDir, String archiveDir) {
        this.dataDir = dataDir;
        this.stagingDir = stagingDir;
        this.destinationDir = destinationDir;
        this.archiveDir = archiveDir;
    } */

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getStagingDir() {
        return stagingDir;
    }

    public void setStagingDir(String stagingDir) {
        this.stagingDir = stagingDir;
    }

    public String getDestinationDir() {
        return destinationDir;
    }

    public void setDestinationDir(String destinationDir) {
        this.destinationDir = destinationDir;
    }

    public String getArchiveDir() {
        return archiveDir;
    }

    public void setArchiveDir(String archiveDir) {
        this.archiveDir = archiveDir;
    }

}
