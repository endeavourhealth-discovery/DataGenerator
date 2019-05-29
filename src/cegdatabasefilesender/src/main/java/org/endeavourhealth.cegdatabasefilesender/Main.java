package org.endeavourhealth.cegdatabasefilesender;

// import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.QueuedMessageDalI;
// import org.endeavourhealth.core.database.rdbms.audit.models.RdbmsQueuedMessage;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.scheduler.job.EncryptFiles;
// import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.endeavourhealth.scheduler.util.SftpConnection;
import org.hibernate.internal.SessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.persistence.EntityManager;
import java.io.File;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.apache.commons.io.FileUtils;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    // private static Main instance = null;

    /* private Main() {
    } */

    /* public static Main getInstance() {
        if (instance == null)
            instance = new Main();

        return instance;
    } */

    public static void main(String[] args) throws Exception {

        // ConfigManager.Initialize("ceg-database-file-sender");
        // Main main = Main.getInstance();

        // TODO Amend pathnames as required
        File dataDir = new File("C:/JDBC/Data/");
        File stagingDir = new File("C:/JDBC/Staging/");

        LOG.info("**********");
        LOG.info("Starting Process.");

        LOG.info("**********");
        LOG.info("Getting stored zipped CSV files from audit.queued_message table, to write to data directory.");

        try {
            ResultSet results = checkAuditQueuedMessageTableForUUIDs();

            while (results.next()) {
                UUID queuedMessageId = UUID.fromString(results.getString("id"));

                try {
                    byte[] bytes = getZipFileByteArrayFromQueuedMessageTable(queuedMessageId);

                    try {
                        writeZipFileToDataDirectory(bytes, queuedMessageId, dataDir);

                        try {
                            // TODO Uncomment out the line of code below when sure that this all works
                            //  Once the file has been taken from audit.queued_message it can be deleted
                            // deleteZipFileByteArrayFromQueuedMessageTable(queuedMessageId);

                        } catch (Exception ex) {
                            LOG.info("**********");
                            LOG.error("Error encountered in deleting zip file from audit.queued_message table: " + ex.getMessage());
                            LOG.error("For UUID: " + queuedMessageId);
                            System.exit(-1);
                        }

                    } catch (Exception ex) {
                        LOG.info("**********");
                        LOG.error("Error encountered in writing zip file to data directory: " + ex.getMessage());
                        LOG.error("For UUID: " + queuedMessageId);
                        System.exit(-1);
                    }

                } catch (Exception ex) {
                    LOG.info("**********");
                    LOG.error("Error encountered in getting zip file from audit.queued_message table: " + ex.getMessage());
                    LOG.error("For UUID: " + queuedMessageId);
                    System.exit(-1);
                }
            }

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in getting UUIDs from audit.queued_message table: " + ex.getMessage());
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Checking contents of data directory for zipped CSV files, to put into a multi-part zip in staging directory.");

        try {
            zipAllContentsOfDataDirectoryToStaging(dataDir, stagingDir);

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in zipping contents of data directory to staging directory: " + ex.getMessage());
            System.exit(-1);
        }

        // LOG.info("**********");
        // LOG.info("Deleting contents of data directory.");
        // TODO Put code here

        LOG.info("**********");
        LOG.info("Checking staging directory for first part of multi-part zip file, to PGP encrypt it.");

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            File zipFile = new File(stagingDir.getAbsolutePath() + File.separator +
                     sdf.format(new Date()) + "_" + "Subscriber_Data" + ".zip");
            if (!encryptFile(zipFile)) {
                LOG.info("**********");
                LOG.error("Unable to encrypt the first part of multi-part zip file in staging directory.");
                System.exit(-1);
            }

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in PGP encrypting first part of multi-part zip file in staging directory: " + ex.getMessage());
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Checking staging directory to send contents to CEG SFTP location.");

        try {
            ConnectionDetails con = setSubscriberConfigSftpConnectionDetails();
            SftpConnection sftp = new SftpConnection(con);

            try {
                sftp.open();
                // LOG.info("**********");
                // LOG.info("SFTP connection opened.");

                try {
                    // TODO Amend SFTP location as required
                    String location = "/endeavour/ftp/Test/";
                    File[] files = stagingDir.listFiles();
                    // LOG.info("**********");
                    // LOG.info("Starting file/s upload.");
                    for (File file : files) {
                        LOG.info("**********");
                        LOG.info("Uploading file: " + file.getName());
                        sftp.put(file.getAbsolutePath(), location);
                    }
                    sftp.close();

                } catch (Exception ex) {
                    LOG.info("**********");
                    LOG.error("Error encountered while uploading to the SFTP: " + ex.getMessage());
                    System.exit(-1);
                }

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered while connecting to the SFTP: " + ex.getMessage());
                System.exit(-1);
            }

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Exception occurred while reading clientPrivateKey file. " + ex);
            System.exit(-1);
        }

        // LOG.info("**********");
        // LOG.info("Archiving contents of staging directory.");
        // TODO Put code here
        File archiveDir = new File("C:/JDBC/Archive/");

        LOG.info("**********");
        LOG.info("Process Completed.");
        System.exit(0);

    }

    private static ResultSet checkAuditQueuedMessageTableForUUIDs() throws Exception {

        EntityManager entityManager = ConnectionManager.getAuditEntityManager();
        PreparedStatement ps = null;
        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            String sql = "select id"
                    + " from"
                    + " queued_message";

            ps = connection.prepareStatement(sql);
            ps.executeQuery();
            ResultSet results = ps.getResultSet();
            return results;

        } catch (SQLException ex) {
            throw ex;
        }
    }

    private static byte[] getZipFileByteArrayFromQueuedMessageTable(UUID queuedMessageId) throws Exception {
        QueuedMessageDalI queuedMessageDal = DalProvider.factoryQueuedMessageDal();
        try {
            String payload = queuedMessageDal.getById(queuedMessageId);
            byte[] bytes = Base64.getDecoder().decode(payload);
            return bytes;

        } catch (Exception ex) {
            throw ex;
        }
    }

    private static void deleteZipFileByteArrayFromQueuedMessageTable (UUID queuedMessageId) throws Exception {
        QueuedMessageDalI queuedMessageDal = DalProvider.factoryQueuedMessageDal();
        try {
            queuedMessageDal.delete(queuedMessageId);

        } catch (Exception ex) {
            throw ex;
        }
    }

    private static void writeZipFileToDataDirectory(byte[] bytes, UUID queuedMessageId, File dataDir) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        File file = new File(dataDir.getAbsolutePath() +
                File.separator +
                sdf.format(new Date()) + "_" +
                queuedMessageId.toString() +
                "_Subscriber_Zip.zip");

        // Full UUID needs to be used in the filename because several different files can be
        // written in the same millisecond so using the code below to attempt filename
        // uniqueness causes files to be overwritten, when this method is in a loop
           /* SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HHmmssSSS");
              File file = new File("C:/JDBC/Output/" +
                sdf.format(new Date()) + "_" +
                "Subscriber_File.zip"); */

        try {
            FileUtils.writeByteArrayToFile(file, bytes);
            // LOG.info("Written ZIP file to " + file);

        } catch (Exception ex) {
            throw ex;
        }
    }

    private static void zipAllContentsOfDataDirectoryToStaging(File dataDir, File staging) throws Exception {

        // LOG.info("**********");
        // LOG.info("Compressing contents of: " + dataDir.getAbsolutePath());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        ZipFile zipFile = new ZipFile(staging + File.separator +
                sdf.format(new Date()) + "_" + "Subscriber_Data" + ".zip");
        // LOG.info("**********");
        // LOG.info("Creating file: " + zipFile.getFile().getAbsolutePath());

        // Set the zip file parameters
        ZipParameters parameters = new ZipParameters();
        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
        parameters.setIncludeRootFolder(false);

        // Create the multi-part zip file from the files in the
        // specified folder, using the zip file parameters
        zipFile.createZipFileFromFolder(dataDir, parameters, true, 10485760);

        // LOG.info("**********");
        // LOG.info(staging.listFiles().length + " Multi-part zip file/s successfully created.");
    }

    private static boolean encryptFile(File file) throws Exception {

        X509Certificate certificate = null;
        try {
            Security.addProvider(new BouncyCastleProvider());
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509", "BC");
            certificate =
                    (X509Certificate) certFactory.generateCertificate(
                            EncryptFiles.class.getClassLoader().getResourceAsStream("discovery.cer"));

        } catch (CertificateException ex) {
            LOG.info("**********");
            LOG.error("Error encountered in certificate generation: " + ex.getMessage());
            throw ex;

        } catch (NoSuchProviderException ex) {
            LOG.info("**********");
            LOG.error("Error encountered in certificate provider: " + ex.getMessage());
            throw ex;
        }

        // LOG.info("**********");
        // LOG.info("Encrypting the file: " + file.getAbsolutePath());

        LOG.info("**********");
        return PgpEncryptDecrypt.encryptFile(file, certificate, "BC");
    }

    private static ConnectionDetails setSubscriberConfigSftpConnectionDetails() throws Exception {
    // private static ConnectionDetails setSubscriberConfigSftpConnectionDetails(ExtractConfig config) {

        // TODO Amend SFTP logon details as required
        // Setting up the connection details

        String hostname = "10.0.101.239";
        // String hostname = config.getSftpConnectionDetails().getHostname();

        int port = 22;
        // int port = config.getSftpConnectionDetails().getPort();

        String username = "endeavour";
        // String username = config.getSftpConnectionDetails().getUsername();

        String clientPrivateKey = null;
        try {
            clientPrivateKey = FileUtils.readFileToString(
                    new File("C:/JDBC/SFTPKey/sftp02endeavour.ppk"), (String) null);

        } catch (Exception ex) {
            throw ex;
        }
        // String clientPrivateKey = config.getSftpConnectionDetails().getClientPrivateKey();

        String clientPrivateKeyPassword = "";
        // String clientPrivateKeyPassword = config.getSftpConnectionDetails().getClientPrivateKeyPassword();

        String hostPublicKey = "";
        // String hostPublicKey = config.getSftpConnectionDetails().getHostPublicKey();

        ConnectionDetails sftpConnectionDetails = new ConnectionDetails();
        sftpConnectionDetails.setHostname(hostname);
        sftpConnectionDetails.setPort(port);
        sftpConnectionDetails.setUsername(username);
        sftpConnectionDetails.setClientPrivateKey(clientPrivateKey);
        sftpConnectionDetails.setClientPrivateKeyPassword(clientPrivateKeyPassword);
        sftpConnectionDetails.setHostPublicKey(hostPublicKey);
        return sftpConnectionDetails;
    }

    private static void helperMethod() {

    }

}
