package org.endeavourhealth.scheduler.json.ExtractDefinition;

public class ExtractConfig {
    private String name = null;
    private Integer id = null;
    private FileLocationDetails fileLocationDetails;
    private SftpConnectionDetails sftpConnectionDetails;

    private ExtractConfig() {}

    private ExtractConfig(String name, Integer id,
                          FileLocationDetails fileLocationDetails,
                          SftpConnectionDetails sftpConnectionDetails) {
        this.name = name;
        this.id = id;
        this.fileLocationDetails = fileLocationDetails;
        this.sftpConnectionDetails = sftpConnectionDetails;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public FileLocationDetails getFileLocationDetails() {
        return fileLocationDetails;
    }

    public void setFileLocationDetails(FileLocationDetails fileLocationDetails) {
        this.fileLocationDetails = fileLocationDetails;
    }

    public SftpConnectionDetails getSftpConnectionDetails() {
        return sftpConnectionDetails;
    }

    public void setSftpConnectionDetails(SftpConnectionDetails sftpConnectionDetails) {
        this.sftpConnectionDetails = sftpConnectionDetails;
    }
}