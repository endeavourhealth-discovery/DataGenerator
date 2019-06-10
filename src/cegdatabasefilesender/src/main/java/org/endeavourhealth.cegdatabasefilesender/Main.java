package org.endeavourhealth.cegdatabasefilesender;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.cegdatabasefilesender.feedback.FeedbackSlurper;
import org.endeavourhealth.common.cache.ObjectMapperPool;
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
import java.io.FileInputStream;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;

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

        int subscriberId = 1;

        SubscriberFileSenderConfig config = getConfig(subscriberId);

        try (FeedbackSlurper feedbackSlurper = new FeedbackSlurper(config)) {
            feedbackSlurper.slurp();
        } catch (Exception e) {
            LOG.error("Cannot slurp feedback", e);
        }

        /* try {
            sendFiles(config);
        } catch (Exception e) {
            LOG.error("Cannot send files", e);
        } */

    }

    private static SubscriberFileSenderConfig getConfig(int subscriberId) throws Exception {

        SubscriberFileSenderEntity sfse = SubscriberFileSenderEntity.getSubscriberFileSenderEntity(subscriberId);
        String definition = sfse.getDefinition();

        SubscriberFileSenderConfig config = null;
        if (!StringUtils.isEmpty(definition)) {
            config = ObjectMapperPool.getInstance().
                    readValue(definition, SubscriberFileSenderConfig.class);
        }

        return config;
    }

    private static void sendFiles(SubscriberFileSenderConfig config) throws Exception {

        // ConfigManager.Initialize("ceg-database-file-sender");
        // Main main = Main.getInstance();

        List<UUID> resultSetUuidsList = new ArrayList<>();
        int subscriberId = 1;

        try {

            String dataDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getDataDir());
            File dataDir = new File(dataDirString);
            makeDirectory(dataDir);

            String stagingDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getStagingDir());
            File stagingDir = new File(stagingDirString);
            makeDirectory(stagingDir);

            String destinationDir = // addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getDestinationDir(); // );

            String archiveDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getArchiveDir());
            File archiveDir = new File(archiveDirString);
            makeDirectory(archiveDir);

            String pgpFile = config.getSubscriberFileLocationDetails().getPgpCertFile();
            File pgpCertFile = new File(pgpFile);

            // Can set the directories without using the database, and amend
            // the pathnames below, if needed, when working on this locally

            // File dataDir = new File("C:/Subscriber/Data/");
            // File stagingDir = new File("C:/Subscriber/Staging/");
            // String destinationDir = "/endeavour/ftp/Test/";
            // File archiveDir = new File("C:/Subscriber/Archive/");
            // File pgpCertDir = new File("C:/Subscriber/PGPCert/");

            LOG.info("**********");
            LOG.info("Starting Process.");

            LOG.info("**********");
            LOG.info("Getting stored zipped CSV files from data_generator.subscriber_zip_file_uuids table, to write to data directory.");

            try {
                // below method call commented out as the payload has already been written
                // to the data_generator.subscriber_zip_file_uuids table by SubscriberFiler
                // ResultSet results = checkAuditQueuedMessageTableForUUIDs();

                // LOG.info("**********");
                // LOG.info("Getting UUIDs from data_generator.subscriber_zip_file_uuids table.");

                ResultSet resultSet = getUUIDsFromDataGenTable(subscriberId);

                while (resultSet.next()) {

                    long filenameCounter = resultSet.getLong("filing_order");
                    UUID queuedMessageId = UUID.fromString(resultSet.getString("queued_message_uuid"));
                    resultSetUuidsList.add(queuedMessageId);

                    try {
                        // below method call commented out as the payload has already been written
                        // to the data_generator.subscriber_zip_file_uuids table by SubscriberFiler
                        // byte[] bytes = getZipFileByteArrayFromQueuedMessageTable(queuedMessageId);

                        byte[] bytes = Base64.getDecoder().decode(resultSet.getString("queued_message_body"));

                        try {
                            writeZipFileToDataDirectory(bytes, filenameCounter, queuedMessageId, dataDir);

                            // below no longer needed as the payload has already been written to the
                            // data_generator.subscriber_zip_file_uuids table by SubscriberFiler, and deleted by it
                                /* try {
                                       deleteEntryFromQueuedMessageTable(queuedMessageId);

                                } catch (Exception ex) {
                                    LOG.info("**********");
                                    LOG.error("Error encountered in deleting zip file from audit.queued_message table: " + ex.getMessage());
                                    LOG.error("For UUID: " + queuedMessageId);
                                    System.exit(-1);
                                } */

                        } catch (Exception ex) {
                            LOG.info("**********");
                            LOG.error("Error encountered in writing zip file to data directory: " + ex.getMessage());
                            LOG.error("For UUID: " + queuedMessageId);
                            System.exit(-1);
                        }

                    } catch (Exception ex) {
                        LOG.info("**********");
                        LOG.error("Error encountered in getting zip file from data_generator.subscriber_zip_file_uuids table: " + ex.getMessage());
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
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                File zipFile = new File(stagingDir.getAbsolutePath() + File.separator +
                        sdf.format(new Date()) + "_" + "Subscriber_Data" + ".zip");
                if (!encryptFile(zipFile, pgpCertFile)) {
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

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered with accessing data_generator.subscriber_file_sender table: " + ex.getMessage());
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Updating data_generator.subscriber_zip_file_uuids table.");

        for (UUID uuid : resultSetUuidsList) {

            String uuidString = uuid.toString();

            try {
                updateFileSentDateTimeInUUIDsTable(uuidString);

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered in updating file_sent entries of data_generator.subscriber_zip_file_uuids table." + ex.getMessage());
                System.exit(-1);
            }

            // below commented out as this is now done by the feedback slurper
            /* try {
                 deleteQueuedMessageBodyFromUUIDsTable(uuidString);

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered in deleting queued_message_body entries of data_generator.subscriber_zip_file_uuids table." + ex.getMessage());
                System.exit(-1);
            } */

        }

        resultSetUuidsList.clear();

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

    /* private static ResultSet checkAuditQueuedMessageTableForUUIDs() throws Exception {

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
    } */

    /* private static byte[] getZipFileByteArrayFromQueuedMessageTable(UUID queuedMessageId) throws Exception {
        QueuedMessageDalI queuedMessageDal = DalProvider.factoryQueuedMessageDal();

        try {
            String payload = queuedMessageDal.getById(queuedMessageId);
            byte[] bytes = Base64.getDecoder().decode(payload);
            return bytes;

        } catch (Exception ex) {
            throw ex;
        }
    } */

    /* private static void deleteEntryFromQueuedMessageTable(UUID queuedMessageId) throws Exception {
        QueuedMessageDalI queuedMessageDal = DalProvider.factoryQueuedMessageDal();

        try {
            queuedMessageDal.delete(queuedMessageId);

        } catch (Exception ex) {
            throw ex;
        }
    } */

    private static ResultSet getUUIDsFromDataGenTable(int subscriberId) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            String sql = "select queued_message_uuid, queued_message_body, filing_order"
                    + " from data_generator.subscriber_zip_file_uuids"
                    + " where subscriber_id = ?"
                    + " and file_sent is null"
                    + " order by filing_order";

            ps = connection.prepareStatement(sql);
            ps.setInt(1, subscriberId);
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();
            return resultSet;

        } catch (Exception ex) {
            throw ex;

        } /* finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
        } */
    }

    private static void updateFileSentDateTimeInUUIDsTable(String queuedMessageId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            java.util.Date date = new java.util.Date();
            Timestamp timestamp = new java.sql.Timestamp(date.getTime());

            String sql = "update data_generator.subscriber_zip_file_uuids"
                    + " set file_sent = ?"
                    + " where queued_message_uuid = ?";

            ps = connection.prepareStatement(sql);
            ps.setTimestamp(1, timestamp);
            ps.setString(2, queuedMessageId);
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

    /* private static void deleteQueuedMessageBodyFromUUIDsTable(String queuedMessageId) throws Exception {
        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            String sql = "update data_generator.subscriber_zip_file_uuids"
                    + " set queued_message_body = null"
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

    } */

    private static void writeZipFileToDataDirectory(byte[] bytes, long filenameCounter,
                                                    UUID queuedMessageId, File dataDir) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        File file = new File(dataDir.getAbsolutePath() +
                File.separator +
                sdf.format(new Date()) + "_" +
                String.format("%014d", filenameCounter) + "_" +
                queuedMessageId.toString() +
                ".zip");

        try {
            FileUtils.writeByteArrayToFile(file, bytes);
            // LOG.info("Written ZIP file to " + file);

        } catch (Exception ex) {
            throw ex;
        }
    }

    private static void zipAllContentsOfDataDirectoryToStaging(File dataDir, File stagingDir) throws Exception {

        // LOG.info("**********");
        // LOG.info("Compressing contents of: " + dataDir.getAbsolutePath());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        ZipFile zipFile = new ZipFile(stagingDir + File.separator +
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
        // LOG.info(stagingDir.listFiles().length + " Multi-part zip file/s successfully created.");
    }

    private static boolean encryptFile(File file, File cert) throws Exception {

        X509Certificate certificate = null;

        try {
            Security.addProvider(new BouncyCastleProvider());
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509", "BC");
            certificate =
                    (X509Certificate) certFactory.generateCertificate(new FileInputStream(cert));

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

            // Can hard code the SFTP details without using the
            // database, if needed, when working on this locally

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
