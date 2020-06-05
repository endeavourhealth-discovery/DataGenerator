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
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class ConceptSender {

    private static final Logger LOG = LoggerFactory.getLogger(ConceptSender.class);
    private static final String ALL = "all";

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 9) {
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
            LOG.info("Delta Date in yyyy-mm-dd format or ALL if sending all of the table");

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
        LOG.info("Delta Date        : " + args[8]);

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

        File adhocDir = new File(args[0]);
        if (adhocDir.exists()) {
            FileUtils.deleteDirectory(adhocDir);
        }
        FileUtils.forceMkdir(adhocDir);

        ArrayList<String> concept = getConcept(args[8]);
        createConceptFile(adhocDir, concept, args[7]);
        ArrayList<String> conceptMap = getConceptMap(args[8]);
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

    private static ArrayList<String> getConcept(String date) throws Exception {

        if (!date.equalsIgnoreCase(ALL)) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                sdf.applyPattern(date);
            } catch (Exception e) {
                throw new Exception("Invalid date specified. " + date);
            }
        }

        ArrayList<String> concepts = new ArrayList<>();

        EntityManager entityManager = PersistenceManager.getEntityManager();
        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();
        String sql = "";
        if (date.equalsIgnoreCase(ALL)) {
            sql = "select * from information_model.concept order by dbid asc;";
        } else {
            sql = "select * from information_model.concept where updated > '" + date + "' order by dbid asc;";
        }
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();

        LOG.info("Fetching contents of concept table.");
        String query = "";
        String value = "";
        int i = 0;
        while (resultSet.next()) {

            if (date.equalsIgnoreCase(ALL)) {
                query = "insert into concept values ([dbid],[document],[id],[draft],[name],[description],[scheme],[code],[use_count],[updated]);";
            } else {
                query = "update concept set document = [document], " +
                        "id = [id]," +
                        "draft = [draft]," +
                        "name = [name]," +
                        "description = [description]," +
                        "scheme = [scheme]," +
                        "code = [code]," +
                        "use_count = [use_count]," +
                        "updated = [updated] where dbid = [dbid] " +
                        "if @@ROWCOUNT = 0 " +
                        "insert into concept values ([dbid],[document],[id],[draft],[name],[description],[scheme],[code],[use_count],[updated]);";
            }

            query = query.replace("[dbid]", String.valueOf(resultSet.getLong(1)));
            query = query.replace("[document]", String.valueOf(resultSet.getLong(2)));
            value = resultSet.getString(3);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[id]", "'" +  value + "'");
            } else {
                query = query.replace("[id]", "null");
            }
            query = query.replace("[draft]", String.valueOf(resultSet.getLong(4)));
            value = resultSet.getString(5);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[name]", "'" +  value + "'");
            } else {
                query = query.replace("[name]", "null");
            }
            value = resultSet.getString(6);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[description]", "'" +  value + "'");
            } else {
                query = query.replace("[description]", "null");
            }
            value = resultSet.getString(7);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[scheme]", "'" +  value + "'");
            } else {
                query = query.replace("[scheme]", "null");
            }
            value = resultSet.getString(8);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[code]", "'" +  value + "'");
            } else {
                query = query.replace("[code]", "null");
            }
            query = query.replace("[use_count]", String.valueOf(resultSet.getLong(9)));
            query = query.replace("[updated]", "'" + String.valueOf(resultSet.getTimestamp(10)) + "'");

            concepts.add(query);
            i++;
            if(i % 10000 == 0 ) {
                LOG.info("Records added: " + i);
                System.gc();
            }
        }
        LOG.info("Total records added: " + concepts.size());
        connection.close();
        resultSet.close();
        ps.close();
        return concepts;
    }

    private static String replaceInvalidChars(String value) {
        return value.replace("'", "''");
    }

    private static ArrayList<String> getConceptMap(String date) throws Exception {

        if (!date.equalsIgnoreCase(ALL)) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                sdf.applyPattern(date);
            } catch (Exception e) {
                throw new Exception("Invalid date specified. " + date);
            }
        }

        ArrayList<String> conceptMap = new ArrayList<>();

        EntityManager entityManager = PersistenceManager.getEntityManager();
        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();

        String sql = "";
        if (date.equalsIgnoreCase(ALL)) {
            sql = "select *  from information_model.concept_map order by legacy asc;";
        } else {
            sql = "select *  from information_model.concept_map where updated > '" + date + "' order by legacy asc;";
        }
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        LOG.info("Fetching contents of concept_map table.");

        String query = "";
        int i = 0;
        while (resultSet.next()) {
            if (date.equalsIgnoreCase(ALL)) {
                query = "insert into concept_map values ([legacy],[core],[updated]);";
            } else {
                query = "update concept_map set core = [core], " +
                        "updated = [updated] where legacy = [legacy] " +
                        "if @@ROWCOUNT = 0 " +
                        "insert into concept_map values ([legacy],[core],[updated]);";
            }
            query = query.replace("[legacy]", String.valueOf(resultSet.getLong(1)));
            query = query.replace("[core]", String.valueOf(resultSet.getLong(2)));
            query = query.replace("[updated]", "'" + String.valueOf(resultSet.getTimestamp(3)) + "'");

            conceptMap.add(query);
            i++;
            if(i % 10000 == 0 ) {
                LOG.info("Records added: " + i);
                System.gc();
            }
        }
        LOG.info("Total records added: " + conceptMap.size());
        connection.close();
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