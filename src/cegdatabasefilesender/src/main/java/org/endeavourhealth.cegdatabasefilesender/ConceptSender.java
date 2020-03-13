package org.endeavourhealth.cegdatabasefilesender;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.endeavourhealth.scheduler.util.SftpConnection;
import org.hibernate.internal.SessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class ConceptSender {

    private static final Logger LOG = LoggerFactory.getLogger(ConceptSender.class);

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 8) {
            LOG.error("Invalid number of parameters.");

            LOG.info("Required parameters:");
            LOG.info("Source Directory");
            LOG.info("Hostname");
            LOG.info("Port");
            LOG.info("Username");
            LOG.info("SFTP Location");
            LOG.info("Key File");
            LOG.info("Certificate File");
            LOG.info("Target Schema");

            System.exit(-1);
        }

        LOG.info("Running Concept Sender with the following parameters:");
        LOG.info("Source Directory  : " + args[0]);
        LOG.info("Hostname          : " + args[1]);
        LOG.info("Port              : " + args[2]);
        LOG.info("Username          : " + args[3]);
        LOG.info("SFTP Location     : " + args[4]);
        LOG.info("Key File          : " + args[5]);
        LOG.info("Certificate File  : " + args[6]);
        LOG.info("Target Schema     : " + args[7]);

        ConnectionDetails con = new ConnectionDetails();
        con.setHostname(args[1]);
        con.setPort(Integer.valueOf(args[2]));
        con.setUsername(args[3]);
        try {
            con.setClientPrivateKey(FileUtils.readFileToString(new File(args[5]), (String) null));
            con.setClientPrivateKeyPassword("");
        } catch (IOException e) {
            LOG.error("Unable to read client private key file." + e.getMessage());
            System.exit(-1);
        }

        SftpConnection sftp = new SftpConnection(con);
        try {
            sftp.open();
            LOG.info("SFTP connection established");
            sftp.close();
        } catch (Exception e) {
            LOG.error("Unable to connect to the SFTP server. " + e.getMessage());
            System.exit(-1);
        }

        ArrayList<String> concept = getConcept();
        ArrayList<String> conceptMap = getConceptMap();
        File adhocDir = new File(args[0]);
        if (adhocDir.exists()) {
            FileUtils.deleteDirectory(adhocDir);
        }
        FileUtils.forceMkdir(adhocDir);
        createConceptFile(adhocDir, concept, args[7]);
        createConceptMapFile(adhocDir, conceptMap, args[7]);

        File zipFile = zipAdhocFiles(adhocDir).getFile();
        File cert = new File(args[6]);

        encryptFile(zipFile, cert);

        try {
            sftp.open();
            String location = args[4];
            LOG.info("Starting file upload.");
            sftp.put(zipFile.getAbsolutePath(), location);
            sftp.close();
        } catch (Exception e) {
            LOG.error("Unable to do SFTP operation. " + e.getMessage());
            System.exit(-1);
        }
        LOG.info("Process completed.");
        System.exit(0);
    }

    private static void createConceptFile(File adhocDir, ArrayList<String> concept, String schema) throws Exception {
        File conceptFile = new File(adhocDir.getAbsolutePath() + File.separator + "concept.sql");
        conceptFile.createNewFile();
        LOG.info("Generating concept sql.");
        FileWriter writer = new FileWriter(conceptFile);
        writer.write("use " + schema + ";" + System.lineSeparator());
        for(String str: concept) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static void createConceptMapFile(File adhocDir, ArrayList<String> conceptMap, String schema) throws Exception {
        File conceptMapFile = new File(adhocDir.getAbsolutePath() + File.separator + "concept_map.sql");
        conceptMapFile.createNewFile();
        LOG.info("Generating concept_map sql.");
        FileWriter writer = new FileWriter(conceptMapFile);
        writer.write("use " + schema + ";" + System.lineSeparator());
        for(String str: conceptMap) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static ArrayList<String> getConcept() throws Exception {

        ArrayList<String> concepts = new ArrayList<>();

        EntityManager entityManager = PersistenceManager.getEntityManager();
        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();
        String sql = "select * from data_sharing_manager.concept order by dbid asc;";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();

        LOG.info("Fetching contents of concept table.");
        int i = 0;
        while (resultSet.next()) {
            String insert = "insert into concept values ([dbid],[document],[id],[draft],[name],[description],[scheme],[code],[use_count]);";
            insert = insert.replace("[dbid]", String.valueOf(resultSet.getLong(1)));
            insert = insert.replace("[document]", String.valueOf(resultSet.getLong(2)));

            String value = resultSet.getString(3);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                insert = insert.replace("[id]", "'" +  value + "'");
            } else {
                insert = insert.replace("[id]", "null");
            }

            insert = insert.replace("[draft]", String.valueOf(resultSet.getLong(4)));
            value = resultSet.getString(5);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                insert = insert.replace("[name]", "'" +  value + "'");
            } else {
                insert = insert.replace("[name]", "null");
            }
            value = resultSet.getString(6);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                insert = insert.replace("[description]", "'" +  value + "'");
            } else {
                insert = insert.replace("[description]", "null");
            }
            value = resultSet.getString(7);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                insert = insert.replace("[scheme]", "'" +  value + "'");
            } else {
                insert = insert.replace("[scheme]", "null");
            }
            value = resultSet.getString(8);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                insert = insert.replace("[code]", "'" +  value + "'");
            } else {
                insert = insert.replace("[code]", "null");
            }
            insert = insert.replace("[use_count]", String.valueOf(resultSet.getLong(9)));
            concepts.add(insert);

            /*
            i++;
            if (i == 10) {
                break;
            }
             */
        }
        resultSet.close();
        ps.close();
        return concepts;
    }

    private static String replaceInvalidChars(String value) {
        return value.replace("'", "''");
    }

    private static ArrayList<String> getConceptMap() throws Exception {

        ArrayList<String> conceptMap = new ArrayList<>();

        EntityManager entityManager = PersistenceManager.getEntityManager();
        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();
        String sql = "select *  from data_sharing_manager.concept_map order by core asc;";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        LOG.info("Fetching contents of concept_map table.");
        int i = 0;
        while (resultSet.next()) {
            String insert = "insert into concept_map values ([legacy],[core]);";
            insert = insert.replace("[legacy]", String.valueOf(resultSet.getLong(1)));
            insert = insert.replace("[core]", String.valueOf(resultSet.getLong(2)));
            conceptMap.add(insert);

            /*
            i++;
            if (i == 10) {
                break;
            }
             */
        }
        resultSet.close();
        ps.close();
        return conceptMap;
    }

    private static ZipFile zipAdhocFiles(File dataDir) throws Exception {
        File zip = new File(dataDir.getParentFile().getAbsolutePath() + File.separator + "adhoc" + ".zip");
        if (zip.exists()) {
            FileUtils.forceDelete(zip);
        }
        ZipFile zipFile = new ZipFile(zip);
        ZipParameters parameters = new ZipParameters();
        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
        parameters.setIncludeRootFolder(false);
        zipFile.createZipFileFromFolder(dataDir, parameters, false, 0);
        return zipFile;
    }

    private static boolean encryptFile(File file, File cert) throws Exception {
        X509Certificate certificate = null;
        try {
            Security.addProvider(new BouncyCastleProvider());
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509", "BC");
            certificate =
                    (X509Certificate) certFactory.generateCertificate(new FileInputStream(cert));

        } catch (CertificateException ex) {
            LOG.error("Error encountered in certificate generation. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in certificate generation. ", ex);
            throw ex;

        } catch (NoSuchProviderException ex) {
            LOG.error("Error encountered in certificate provider. " + ex.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error encountered in certificate provider. ", ex);
            throw ex;
        }
        return PgpEncryptDecrypt.encryptFile(file, certificate, "BC");
    }
}