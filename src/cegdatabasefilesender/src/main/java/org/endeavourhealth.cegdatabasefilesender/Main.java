package org.endeavourhealth.cegdatabasefilesender;

import org.endeavourhealth.cegdatabasefilesender.feedback.FeedbackSlurper;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.scheduler.json.SubscriberFileSenderDefinition.SubscriberFileSenderConfig;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.SubscriberFileSenderEntity;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.endeavourhealth.scheduler.util.SftpConnection;
import org.hibernate.internal.SessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.persistence.EntityManager;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final int SENDING_BATCH_SIZE = 5000;
    private static final int UUIDS_LIMIT = 100000;

    // private static Main instance = null;

    /* private Main() {
    } */

    /* public static Main getInstance() {
        if (instance == null)
            instance = new Main();

        return instance;
    } */

    public static void main(String[] args) throws Exception {

        LOG.info("**********");
        LOG.info("Starting Subscriber Sender App.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Starting Subscriber Sender App.");

        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;
        entityManager.getTransaction().begin();
        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();

        String sql = "select subscriber_id"
                + " from data_generator.subscriber_file_sender"
                + " where subscriber_live is true";

        ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();

        while (resultSet.next()) {

            int subscriberId = (resultSet.getInt("subscriber_id"));

            SubscriberFileSenderConfig config = getConfig(subscriberId);

            /*String slackWebhook = config.getSlackWebhook();
              SlackHelper.setupConfig("", "",
                    SlackHelper.Channel.RemoteFilerAlerts.getChannelName(),
                    slackWebhook);*/

            if (args.length != 1) {
                LOG.error("Need to indicate run mode parameter. [sending] or [feedback].");
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Need to indicate run mode parameter. [sending] or [feedback].");
            }

            if (args[0].equalsIgnoreCase("feedback")) {
                try (FeedbackSlurper feedbackSlurper = new FeedbackSlurper(config)) {
                    feedbackSlurper.slurp(subscriberId);
                } catch (Exception ex) {
                    LOG.error("Cannot process feedback results. ", ex);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Cannot process feedback results. ", ex);
                }
            }

            if (args[0].equalsIgnoreCase("sending")) {
                try {
                    sendFiles(config, subscriberId);

                } catch (Exception ex) {
                    LOG.error("Cannot send files. ", ex);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Cannot send files. ", ex);
                }
            }

        }
        resultSet.close();
        ps.close();

        LOG.info("**********");
        LOG.info("Ending Subscriber Sender App.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Ending Subscriber Sender App.");

        System.exit(0);
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

    private static void sendFiles(SubscriberFileSenderConfig config, int subscriberId) throws Exception {

        // ConfigManager.Initialize("ceg-database-file-sender");
        // Main main = Main.getInstance();

        // int subscriberId = 1;

        List<UUID> resultSetUuidsList = new ArrayList<>();
        boolean maxBatchSizeSent = false;

        LOG.info("**********");
        LOG.info("Start of sending process for subscriber_id {}.", subscriberId);
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "*** Start of sending process for subscriber_id " + subscriberId);

        try {

            String dataDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getDataDir());
            File dataDir = new File(dataDirString);
            FileUtils.deleteDirectory(dataDir);
            makeDirectory(dataDir);

            String stagingDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getStagingDir());
            File stagingDir = new File(stagingDirString);
            FileUtils.deleteDirectory(stagingDir);
            makeDirectory(stagingDir);

            String destinationDir = // addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getDestinationDir(); // );

            String archiveDirString = addFileSeparatorToEndOfDirString(
                    config.getSubscriberFileLocationDetails().getArchiveDir());
            File archiveDir = new File(archiveDirString);
            makeDirectory(archiveDir);

            String pgpFile = config.getSubscriberFileLocationDetails().getPgpCertFile();
            File pgpCertFile = new File(pgpFile);

            // String privateFile = config.getSubscriberFileLocationDetails().getPrivateKeyFile();
            // File privateKeyFile = new File(privateFile);

            // Can set the directories without using the database, and amend
            // the pathnames below, if needed, when working on this locally

            // File dataDir = new File("C:/Subscriber/Data/");
            // File stagingDir = new File("C:/Subscriber/Staging/");
            // String destinationDir = "/endeavour/ftp/Test/";
            // File archiveDir = new File("C:/Subscriber/Archive/");
            // File pgpCertDir = new File("C:/Subscriber/PGPCert/");

            LOG.info("**********");
            LOG.info("Getting unsent UUIDs count from data_generator.subscriber_zip_file_uuids table.");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Getting unsent UUIDs count from data_generator.subscriber_zip_file_uuids table.");

            try {
                // below method call commented out as the payload has already been written
                // to the data_generator.subscriber_zip_file_uuids table by SubscriberFiler
                // ResultSet results = checkAuditQueuedMessageTableForUUIDs();

                // LOG.info("**********");
                // LOG.info("Getting UUIDs from data_generator.subscriber_zip_file_uuids table.");

                int unsentUUIDs = 0;
                unsentUUIDs = getUnsentUUIDsCountFromDataGenTable(subscriberId);

                LOG.info("**********");
                LOG.info("Unsent UUIDs: ?", unsentUUIDs);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Unsent UUIDs: " + unsentUUIDs);

                int processingCycles = unsentUUIDs / UUIDS_LIMIT;
                if (!(unsentUUIDs % UUIDS_LIMIT == 0)) {
                    processingCycles++;
                }

                for (int j = 0; j < processingCycles; j++) {

                    LOG.info("**********");
                    LOG.info("Getting payload sets of zipped CSV files from data_generator.subscriber_zip_file_uuids table, to write to data directory, up to limit of: ?", UUIDS_LIMIT);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Getting payload sets of zipped CSV files from data_generator.subscriber_zip_file_uuids table, up to limit of: " + UUIDS_LIMIT);

                    try {

                        // ResultSet resultSet = getUUIDsFromDataGenTable(subscriberId);
                        List<Payload> payloadList = getPayloadsFromDataGenTable(subscriberId);

                        int sendBatchSizeCounter = 0;

                        // while (resultSet.next()) {

                        for (Payload payload : payloadList) {

                            sendBatchSizeCounter++;

                            long filenameCounter = payload.getFilingOrder();
                            UUID queuedMessageId = payload.getQueuedMessageId();
                            resultSetUuidsList.add(queuedMessageId);
                            byte[] bytes = payload.getQueuedMessageBody();

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
                                LOG.error("Error encountered in writing zip file to data directory. " + ex.getMessage());
                                LOG.error("For UUID: " + queuedMessageId);
                                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in writing zip file to data directory. ", ex);
                                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "For UUID: " + queuedMessageId);
                                System.exit(-1);
                            }


                            if (sendBatchSizeCounter % SENDING_BATCH_SIZE == 0) {
                                processZipFilesBatch(dataDir, stagingDir, destinationDir, archiveDir, pgpCertFile, resultSetUuidsList, config);
                                maxBatchSizeSent = true;
                                resultSetUuidsList.clear();
                            }

                        }

                        // resultSet.close();

                    } catch (Exception ex) {
                        LOG.info("**********");
                        // LOG.error("Error encountered in getting UUIDs from audit.queued_message table: " + ex.getMessage());
                        LOG.error("Error encountered in getting payload sets of zipped CSV files from data_generator.subscriber_zip_file_uuids table. " + ex.getMessage());
                        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in getting payload sets of zipped CSV files from data_generator.subscriber_zip_file_uuids table. ", ex);
                        System.exit(-1);
                    }

                }

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered in getting unsent UUIDs count from data_generator.subscriber_zip_file_uuids table. " + ex.getMessage());
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in getting unsent UUIDs count from data_generator.subscriber_zip_file_uuids table. ", ex);
                System.exit(-1);
            }

            if ((dataDir.listFiles().length == 0)) {

                if (!maxBatchSizeSent) {
                    LOG.info("**********");
                    LOG.info("No zip files to send for subscriber_id {}.", subscriberId);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "No zip files to send for subscriber_id " + subscriberId);
                }

            } else {
                processZipFilesBatch(dataDir, stagingDir, destinationDir, archiveDir, pgpCertFile, resultSetUuidsList, config);
            }

            LOG.info("**********");
            LOG.info("End of sending process for subscriber_id {}.", subscriberId);
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "*** End of sending process for subscriber_id " + subscriberId);

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered with accessing data_generator.subscriber_file_sender table. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered with accessing data_generator.subscriber_file_sender table. ", ex);
            System.exit(-1);
        }

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

    private static void processZipFilesBatch(File dataDir, File stagingDir, String destinationDir,
                                             File archiveDir, File pgpCertFile, List<UUID> resultSetUuidsList,
                                             SubscriberFileSenderConfig config) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String fileDate = sdf.format(new Date());

        try {
            zipAllContentsOfDataDirectoryToStaging(fileDate, dataDir, stagingDir);

            LOG.info("**********");
            LOG.info(dataDir.listFiles().length + " sets of zipped CSV files in data directory put into a multi-part zip in staging directory.");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, dataDir.listFiles().length + " sets of zipped CSV files in data directory put into a multi-part zip in staging directory.");

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in zipping contents of data directory to staging directory. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in zipping contents of data directory to staging directory. " + ex.getMessage());
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Deleting contents of data directory.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Deleting contents of data directory.");

        try {
            // TODO Comment out and uncomment the line below as necessary
            FileUtils.cleanDirectory(dataDir);

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in deleting contents of data directory. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in deleting contents of data directory. ", ex);
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Checking staging directory for first part of multi-part zip file, to PGP encrypt it.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Checking staging directory for first part of multi-part zip file, to PGP encrypt it.");

        try {
            File zipFile = new File(stagingDir.getAbsolutePath() + File.separator +
                    fileDate + "_" + "Subscriber_Data" + ".zip");
            if (!encryptFile(zipFile, pgpCertFile)) {
                LOG.info("**********");
                LOG.error("Unable to encrypt the first part of multi-part zip file in staging directory.");
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Unable to encrypt the first part of multi-part zip file in staging directory.");
                System.exit(-1);
            }

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in PGP encrypting first part of multi-part zip file in staging directory. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in PGP encrypting first part of multi-part zip file in staging directory. ", ex);
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Checking staging directory to send contents to CEG SFTP location.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Checking staging directory to send contents to CEG SFTP location.");

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
                        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Uploading file: " + file.getName());
                        sftp.put(file.getAbsolutePath(), destinationDir);
                    }
                    sftp.close();

                } catch (Exception ex) {
                    LOG.info("**********");
                    LOG.error("Error encountered while uploading to the SFTP. " + ex.getMessage());
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered while uploading to the SFTP. ", ex);
                    System.exit(-1);
                }

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered while connecting to the SFTP. " + ex.getMessage());
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered while connecting to the SFTP. ", ex);
                System.exit(-1);

            } finally {
                if (sftp != null)
                    sftp.close();
            }

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered while setting SFTP connection details. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered while setting SFTP connection details. ", ex);
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Archiving contents of staging directory.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Archiving contents of staging directory.");

        try {
            File[] files = stagingDir.listFiles();

            for (File file : files) {

                FileUtils.copyFileToDirectory(file, archiveDir);
                System.gc();
                Thread.sleep(1000);
                Path filepath = file.toPath();
                Files.delete(filepath);
                // FileUtils.forceDelete(file);
            }

            // FileUtils.copyDirectory(stagingDir, archiveDir);
            // FileUtils.cleanDirectory(stagingDir);

        } catch (Exception ex) {
            LOG.info("**********");
            LOG.error("Error encountered in archiving contents of staging directory. " + ex.getMessage());
            // ex.printStackTrace();
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in archiving contents of staging directory. ", ex);
            System.exit(-1);
        }

        LOG.info("**********");
        LOG.info("Updating data_generator.subscriber_zip_file_uuids table.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Updating data_generator.subscriber_zip_file_uuids table.");

        for (UUID uuid : resultSetUuidsList) {

            String uuidString = uuid.toString();

            try {
                updateFileSentDateTimeInUUIDsTable(uuidString);

            } catch (Exception ex) {
                LOG.info("**********");
                LOG.error("Error encountered in updating file_sent entries of data_generator.subscriber_zip_file_uuids table. " + ex.getMessage());
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in updating file_sent entries of data_generator.subscriber_zip_file_uuids table. ", ex);
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

    private static int getUnsentUUIDsCountFromDataGenTable(int subscriberId) throws Exception {

        EntityManager entityManager = PersistenceManager.getEntityManager();
        PreparedStatement ps = null;

        try {
            entityManager.getTransaction().begin();
            SessionImpl session = (SessionImpl) entityManager.getDelegate();
            Connection connection = session.connection();

            String sql = "select count(batch_uuid)"
                    + " from data_generator.subscriber_zip_file_uuids"
                    + " where subscriber_id = ?"
                    + " and file_sent is null"
                    + " order by filing_order";

            ps = connection.prepareStatement(sql);
            ps.clearParameters();
            ps.setInt(1, subscriberId);
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();

            resultSet.first();
            int unsentUUIDs = resultSet.getInt(1);
            resultSet.close();
            connection.close();
            return unsentUUIDs;

        } catch (Exception ex) {
            throw ex;

        } finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
        }

    }

    private static List<Payload> getPayloadsFromDataGenTable(int subscriberId) throws Exception {

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
                    + " order by filing_order"
                    + " limit ?";

            ps = connection.prepareStatement(sql);
            ps.clearParameters();
            ps.setInt(1, subscriberId);
            ps.setInt(2, UUIDS_LIMIT);
            ps.executeQuery();
            ResultSet resultSet = ps.getResultSet();

            List<Payload> payloadList = new ArrayList<>();

            while (resultSet.next()) {

                Payload payload = new Payload();
                payload.setQueuedMessageId(UUID.fromString(resultSet.getString("queued_message_uuid")));
                payload.setFilingOrder(resultSet.getLong("filing_order"));
                payload.setQueuedMessageBody(Base64.getDecoder().decode(resultSet.getString("queued_message_body")));

                payloadList.add(payload);

            }
            resultSet.close();
            connection.close();
            return payloadList;

        } catch (Exception ex) {
            throw ex;

        } finally {
            if (ps != null) {
                ps.close();
            }
            entityManager.close();
        }

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
            ps.clearParameters();
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

    private static void zipAllContentsOfDataDirectoryToStaging(String fileDate, File dataDir, File stagingDir) throws Exception {

        // LOG.info("**********");
        // LOG.info("Compressing contents of: " + dataDir.getAbsolutePath());
        ZipFile zipFile = new ZipFile(stagingDir + File.separator +
                fileDate + "_" + "Subscriber_Data" + ".zip");
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


        LOG.info("**********");
        LOG.info(stagingDir.listFiles().length + " multi-part zip file/s successfully created.");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, stagingDir.listFiles().length + " multi-part zip file/s successfully created.");
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
            LOG.error("Error encountered in certificate generation. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in certificate generation. ", ex);
            throw ex;

        } catch (NoSuchProviderException ex) {
            LOG.info("**********");
            LOG.error("Error encountered in certificate provider. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in certificate provider. ", ex);
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

            // String clientPrivateKey = config.getSftpConnectionDetails().getClientPrivateKey();
            File privateKeyFile = new File(config.getSubscriberFileLocationDetails().getPrivateKeyFile());
            String clientPrivateKey = FileUtils.readFileToString(privateKeyFile, (String) null);

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
