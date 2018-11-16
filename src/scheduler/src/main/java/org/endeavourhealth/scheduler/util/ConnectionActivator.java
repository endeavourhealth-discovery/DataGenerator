package org.endeavourhealth.scheduler.util;

// import com.google.common.base.Strings;

public class ConnectionActivator {

    /**
     * if a host name is provided, it returns an SFTP connection, otherwise a File connection
     */
    public static Connection createConnection(ConnectionDetails connectionDetails) {
        String sftpHostName = connectionDetails.getHostname();
        // Selection from SFTPReader project commented out in DataGenerator as not required
        // if (!Strings.isNullOrEmpty(sftpHostName)) {
            return new SftpConnection(connectionDetails);

        // } else {
        //    return new FileConnection(connectionDetails);
        //}
    }

    /*public static Connection createConnection(String interfaceTypeName, ConnectionDetails connectionDetails) {

        String interfaceType = interfaceTypeName.toUpperCase();
        if (interfaceType.startsWith("EMIS-EXTRACT-SERVICE")) {
            return new SftpConnection(connectionDetails);
        } else if (interfaceType.startsWith("VISION-EXTRACT-SERVICE")) {
            return new SftpConnection(connectionDetails);
        } else {
            return new FileConnection(connectionDetails);
        }
    }*/
}