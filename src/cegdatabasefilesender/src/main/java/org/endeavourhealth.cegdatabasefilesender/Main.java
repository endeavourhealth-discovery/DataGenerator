package org.endeavourhealth.cegdatabasefilesender;

// import org.endeavourhealth.common.config.ConfigManager;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.QueuedMessageDalI;
// import org.endeavourhealth.core.database.rdbms.audit.models.RdbmsQueuedMessage;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.scheduler.job.EncryptFiles;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.SubscriberFileSenderEntity;
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
import java.sql.*;
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

        int subscriberId = 1;

        try {
            SubscriberFileSenderEntity sfse = SubscriberFileSenderEntity.
                    getSubscriberFileSenderEntity(subscriberId);
            String definition = sfse.getDefinition();

            SubscriberFileSenderConfig config = null;
            if (!StringUtils.isEmpty(definition)) {
                config = ObjectMapperPool.getInstance().
                        readValue(definition, SubscriberFileSenderConfig.class);
            }

            String dataDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getDataDir());
            File dataDir = new File(dataDirString);
            makeDirectory(dataDir);

            String stagingDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getStagingDir());
            File stagingDir = new File(stagingDirString);
            makeDirectory(stagingDir);

            String destinationDir = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getDestinationDir());

            String archiveDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getArchiveDir());
            File archiveDir = new File(archiveDirString);
            makeDirectory(archiveDir);

            // TODO Amend pathnames as required
            // File dataDir = new File("C:/Subscriber/Data/");
            // File stagingDir = new File("C:/Subscriber/Staging/");
            // String destinationDir = "/endeavour/ftp/Test/";
            // File archiveDir = new File("C:/Subscriber/Archive/");

            LOG.info("**********");
            LOG.info("Starting Process.");

            LOG.info("**********");
            LOG.info("Getting stored zipped CSV files from audit.queued_message table, to write to data directory.");

            try {
                // ResultSet results = checkAuditQueuedMessageTableForUUIDs();

                LOG.info("**********");
                LOG.info("Getting UUIDs from data_generator.subscriber_zip_file_uuids table.");

                ResultSet resultSet = getUUIDsFromDataGenTable(subscriberId);

                // int filenameCounter = 0;
                while (resultSet.next()) {
                    // filenameCounter++;

                    int filenameCounter = resultSet.getInt("filing_order");
                    UUID queuedMessageId = UUID.fromString(resultSet.getString("queued_message_uuid"));

                    // UUID queuedMessageId = UUID.fromString(results.getString("id"));
                    // Timestamp timestamp = results.getTimestamp("timestamp");
                    // LOG.info("UUID: " + queuedMessageId);
                    // LOG.info("Timestamp: " + timestamp);

                    try {
                        byte[] bytes = getZipFileByteArrayFromQueuedMessageTable(queuedMessageId);

                        try {
                            // writeZipFileToDataDirectory(bytes, queuedMessageId, dataDir);
                            writeZipFileToDataDirectory(bytes, filenameCounter, dataDir);

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
                // LOG.error("Error encountered in getting UUIDs from audit.queued_message table: " + ex.getMessage());
                LOG.error("Error encountered in getting UUIDs from data_generator.subscriber_zip_file_uuids table." + ex.getMessage());
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

            LOG.info("**********");
            LOG.info("Deleting contents of data directory.");

            try {
                // TODO Uncomment out the line of code below as necessary
                // FileUtils.cleanDirectory(dataDir);

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered in deleting contents of data directory: " + ex.getMessage());
                System.exit(-1);
            }

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
                ConnectionDetails con = setSubscriberConfigSftpConnectionDetails(config);
                SftpConnection sftp = new SftpConnection(con);

                try {
                    sftp.open();
                    // LOG.info("**********");
                    // LOG.info("SFTP connection opened.");

                    try {
                        File[] files = stagingDir.listFiles();
                        // LOG.info("**********");
                        // LOG.info("Starting file/s upload.");
                        for (File file : files) {
                            LOG.info("**********");
                            LOG.info("Uploading file: " + file.getName());
                            sftp.put(file.getAbsolutePath(), destinationDir);
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

                } finally {
                    if (sftp != null)
                        sftp.close();
                }

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered while setting SFTP connection details: " + ex.getMessage());
                System.exit(-1);
            }

            LOG.info("**********");
            LOG.info("Archiving contents of staging directory.");

            try {
                FileUtils.copyDirectory(stagingDir, archiveDir);
                FileUtils.cleanDirectory(stagingDir);

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered in archiving contents of staging directory: " + ex.getMessage());
                System.exit(-1);
            }

        } catch (SQLException ex) {
            LOG.info("**********");
            LOG.error("Error encountered with accessing data_generator.subscriber_file_sender table: " + ex.getMessage());
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Updating data_generator.subscriber_zip_file_uuids table.");

        try {
            ResultSet resultSet = getUUIDsFromDataGenTable(subscriberId);
            while (resultSet.next()) {
                String queuedMessageId = resultSet.getString("queued_message_uuid");

                try {
                    updateFileSentToTrueInUUIDTable(queuedMessageId);

                } catch (Exception ex) {
                    LOG.info("**********");
                    LOG.error("Error encountered in updating entries of data_generator.subscriber_zip_file_uuids table." + ex.getMessage());
                    System.exit(-1);
                }
            }

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in getting UUIDs from data_generator.subscriber_zip_file_uuids table." + ex.getMessage());
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Process Completed.");

        System.exit(0);

    }

    private static String addFileSeparatorToEndOfDirString(String dirString) {
        if (!(dirString.endsWith(File.separator))) {
            dirString += File.separator;
        }

        return dirString;
    }

    private static void makeDirectory(File directory) {
        if (!(directory.exists())) {
            directory.mkdirs();
        }
    }

    private static ResultSet checkAuditQueuedMessageTableForUUIDs() throws Exception {

        EntityManager entityManager = ConnectionManager.getAuditEntityManager();
        PreparedStatement ps = null;
        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            String sql = "select id, timestamp"
                    + " from queued_message"
                    + " where queued_message_type_id = 2"
                    + " order by timestamp";

            ps = connection.prepareStatement(sql);
            ps.executeQuery();
            ResultSet results = ps.getResultSet();
            return results;

        } catch (Exception ex) {
            throw ex;

        } finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
        }
    }

    private static ResultSet getUUIDsFromDataGenTable(int subscriberId) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            String sql = "select queued_message_uuid, filing_order"
                    + " from data_generator.subscriber_zip_file_uuids"
                    + " where subscriber_id = ?"
                    + " and file_sent is false"
                    + " order by filing_order";

            ps = connection.prepareStatement(sql);
            ps.setInt(1, subscriberId);
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();
            return resultSet;

        } catch (Exception ex) {
            throw ex;

        } finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
        }
    }

    private static void updateFileSentToTrueInUUIDTable(String queuedMessageId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            String sql = "update data_generator.subscriber_zip_file_uuids"
                    + " set file_sent = true"
                    + " where queued_message_uuid = ?";

            ps = connection.prepareStatement(sql);
            ps.setString(1, queuedMessageId);
            ps.executeUpdate();
            entityManager.getTransaction().commit();

        } catch (Exception ex) {
            entityManager.getTransaction().rollback();
            throw ex;

        } finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
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

    private static void deleteZipFileByteArrayFromQueuedMessageTable(UUID queuedMessageId) throws Exception {
        QueuedMessageDalI queuedMessageDal = DalProvider.factoryQueuedMessageDal();

        try {
            queuedMessageDal.delete(queuedMessageId);

        } catch (Exception ex) {
            throw ex;
        }
    }

    // private static void writeZipFileToDataDirectory(byte[] bytes, UUID queuedMessageId, File dataDir) throws Exception {
    // private static void writeZipFileToDataDirectory(byte[] bytes, Timestamp timestamp, File dataDir) throws Exception {
    private static void writeZipFileToDataDirectory(byte[] bytes, int filenameCounter, File dataDir) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        File file = new File(dataDir.getAbsolutePath() +
                File.separator +
                sdf.format(new Date()) + "_" +
                // queuedMessageId.toString() +
                String.format("%07d", filenameCounter) +
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

    private static ConnectionDetails setSubscriberConfigSftpConnectionDetails(SubscriberFileSenderConfig config) throws Exception {
        // private static ConnectionDetails setSubscriberConfigSftpConnectionDetails() throws Exception {

        try {
            // TODO Amend SFTP logon details as required
            // Setting up the connection details

            // String hostname = "10.0.101.239";
            String hostname = config.getSftpConnectionDetails().getHostname();

            // int port = 22;
            int port = config.getSftpConnectionDetails().getPort();

            // String username = "endeavour";
            String username = config.getSftpConnectionDetails().getUsername();

        /* String clientPrivateKey = null;
        try {
            clientPrivateKey = FileUtils.readFileToString(
                    new File("C:/Subscriber/SFTPKey/sftp02endeavour.ppk"), (String) null);

        } catch (Exception ex) {
            throw ex;
        } */

            String clientPrivateKey = config.getSftpConnectionDetails().getClientPrivateKey();

            // String clientPrivateKeyPassword = "";
            String clientPrivateKeyPassword = config.getSftpConnectionDetails().getClientPrivateKeyPassword();

            // String hostPublicKey = "";
            String hostPublicKey = config.getSftpConnectionDetails().getHostPublicKey();

            ConnectionDetails sftpConnectionDetails = new ConnectionDetails();
            sftpConnectionDetails.setHostname(hostname);
            sftpConnectionDetails.setPort(port);
            sftpConnectionDetails.setUsername(username);
            sftpConnectionDetails.setClientPrivateKey(clientPrivateKey);
            sftpConnectionDetails.setClientPrivateKeyPassword(clientPrivateKeyPassword);
            sftpConnectionDetails.setHostPublicKey(hostPublicKey);

            return sftpConnectionDetails;

        } catch (Exception ex) {
            throw ex;
        }
    }

}
