package org.endeavourhealth.cegdatabasefilesender;

import com.fasterxml.jackson.databind.JsonNode;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.endeavourhealth.scheduler.util.SftpConnection;
import org.hibernate.internal.SessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
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
import java.util.HashMap;
import java.util.Map;

public class ConceptSender {

    private static EntityManagerFactory entityManagerFactory = null;
    private static EntityManager entityManager = null;
    private static final Logger LOG = LoggerFactory.getLogger(ConceptSender.class);
    private static final String MYSQL = "mysql";
    private static final String SQL_SERVER = "sql_server";

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 11) {
            LOG.error("Invalid number of parameters.");

            LOG.info("Required parameters:");
            LOG.info("Source Directory");
            LOG.info("Archive Directory");
            LOG.info("Hostname");
            LOG.info("Port");
            LOG.info("Username");
            LOG.info("SFTP Location");
            LOG.info("Key File");
            LOG.info("Certificate File");
            LOG.info("Target Schema");
            LOG.info("Target Database Server");
            LOG.info("Delta Date in yyyy-mm-dd format");

            System.exit(-1);
        }

        LOG.info("Running Concept Sender with the following parameters:");
        LOG.info("Source Directory       : " + args[0]);
        LOG.info("Archive Directory      : " + args[1]);
        LOG.info("Hostname               : " + args[2]);
        LOG.info("Port                   : " + args[3]);
        LOG.info("Username               : " + args[4]);
        LOG.info("SFTP Location          : " + args[5]);
        LOG.info("Key File               : " + args[6]);
        LOG.info("Certificate File       : " + args[7]);
        LOG.info("Target Schema          : " + args[8]);
        LOG.info("Target Database Server : " + args[9]);
        LOG.info("Delta Date             : " + args[10]);

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.applyPattern(args[10]);
        } catch (Exception e) {
            throw new Exception("Invalid date specified. " + args[9]);
        }

        if (args[10].equalsIgnoreCase("mysql") && args[9].equalsIgnoreCase("sql_server")) {
            throw new Exception("Invalid Target Database Server specified: " + args[9] + " . Only mysql or sql_server is allowed");
        }

        JsonNode json = ConfigManager.getConfigurationAsJson("database", "information-model");
        String url = json.get("url").asText();
        String user = json.get("username").asText();
        String pass = json.get("password").asText();

        Map<String, Object> properties = new HashMap<>();
        //properties.put("hibernate.temp.use_jdbc_metadata_defaults", "false");
        properties.put("hibernate.hikari.dataSource.url", url);
        properties.put("hibernate.hikari.dataSource.user", user);
        properties.put("hibernate.hikari.dataSource.password", pass);
        properties.put("javax.persistence.provider", "org.hibernate.jpa.HibernatePersistenceProvider");
        properties.put("hibernate.connection.provider_class", "org.hibernate.hikaricp.internal.HikariCPConnectionProvider");
        entityManagerFactory = Persistence.createEntityManagerFactory("information_model", properties);
        entityManager = entityManagerFactory.createEntityManager();

        ConnectionDetails con = new ConnectionDetails();
        con.setHostname(args[2]);
        con.setPort(Integer.valueOf(args[3]));
        con.setUsername(args[4]);
        try {
            con.setClientPrivateKey(FileUtils.readFileToString(new File(args[6]), (String) null));
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

        File sourceDir = new File(args[0]);
        if (sourceDir.exists()) {
            FileUtils.deleteDirectory(sourceDir);
        }
        FileUtils.forceMkdir(sourceDir);

        ArrayList<String> concept = getConcept(args[10], args[9]);
        createConceptFile(sourceDir, concept, args[8]);

        ArrayList<String> conceptMap = getConceptMap(args[10], args[9]);
        createConceptMapFile(sourceDir, conceptMap, args[8]);

        ArrayList<String> conceptProperty = getConceptProperty(args[10]);
        createConceptPropertyFile(sourceDir, conceptProperty, args[8]);

        File zipFile = zipAdhocFiles(sourceDir).getFile();
        File cert = new File(args[7]);

        encryptFile(zipFile, cert);

        try {
            sftp.open();
            String location = args[5];
            LOG.info("Starting file upload.");
            sftp.put(zipFile.getAbsolutePath(), location);
            sftp.close();
            File archiveDir = new File(args[1]);
            if (!archiveDir.exists()) {
                FileUtils.forceMkdir(archiveDir);
            }
            File archive = new File(archiveDir.getAbsolutePath() + File.separator +
                    zipFile.getName().substring(0, zipFile.getName().length() - 4) + "_" + args[10] + ".zip");
            FileUtils.copyFile(zipFile, archive);
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
        for (String str : concept) {
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
        for (String str : conceptMap) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static void createConceptPropertyFile(File adhocDir, ArrayList<String> conceptProperty, String schema) throws Exception {
        File conceptMapFile = new File(adhocDir.getAbsolutePath() + File.separator + "concept_property_object.sql");
        conceptMapFile.createNewFile();
        LOG.info("Generating concept_property_object sql.");
        FileWriter writer = new FileWriter(conceptMapFile);
        writer.write("use " + schema + ";" + System.lineSeparator());
        //TODO: For now truncate concept_property_object since we have no way to update concept_property_object table yet since it has no PRIMARY key
        //TODO: We will rebuild all the contents of this table
        writer.write("truncate table concept_property_object" + ";" + System.lineSeparator());
        for (String str : conceptProperty) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static void createConceptTctFile(File adhocDir, ArrayList<String> conceptTct, String schema) throws Exception {
        File conceptTctFile = new File(adhocDir.getAbsolutePath() + File.separator + "concept_tct.sql");
        conceptTctFile.createNewFile();
        LOG.info("Generating concept_tct sql.");
        FileWriter writer = new FileWriter(conceptTctFile);
        writer.write("use " + schema + ";" + System.lineSeparator());
        for (String str : conceptTct) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static ArrayList<String> getConcept(String date, String server) throws Exception {

        ArrayList<String> concepts = new ArrayList<>();

        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();
        //String sql = "select * from information_model.concept order by dbid asc;";
        String sql = "select * from information_model.concept where updated > '" + date + "' order by dbid asc;";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        LOG.info("Fetching contents of concept table.");

        String query = "";
        String value = "";
        int i = 0;
        while (resultSet.next()) {

            if (server.equalsIgnoreCase(SQL_SERVER)) {
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
            } else {
                query = "INSERT INTO concept (`dbid`,`document`,`id`,`draft`,`name`,`description`,`scheme`,`code`,`use_count`,`updated`) " +
                        "VALUES ([dbid],[document],[id],[draft],[name],[description],[scheme],[code],[use_count],[updated]) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "`id` = [id]," +
                        "`draft` = [draft]," +
                        "`name` = [name]," +
                        "`description` = [description]," +
                        "`scheme` = [scheme]," +
                        "`code` = [code]," +
                        "`use_count` = [use_count]," +
                        "`updated` = [updated];";
            }

            query = query.replace("[dbid]", String.valueOf(resultSet.getLong(1)));
            query = query.replace("[document]", String.valueOf(resultSet.getLong(2)));
            value = resultSet.getString(3);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[id]", "'" + value + "'");
            } else {
                query = query.replace("[id]", "null");
            }
            query = query.replace("[draft]", String.valueOf(resultSet.getLong(4)));
            value = resultSet.getString(5);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[name]", "'" + value + "'");
            } else {
                query = query.replace("[name]", "null");
            }
            value = resultSet.getString(6);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[description]", "'" + value + "'");
            } else {
                query = query.replace("[description]", "null");
            }
            value = resultSet.getString(7);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[scheme]", "'" + value + "'");
            } else {
                query = query.replace("[scheme]", "null");
            }
            value = resultSet.getString(8);
            if (StringUtils.isNotEmpty(value)) {
                value = replaceInvalidChars(value);
                query = query.replace("[code]", "'" + value + "'");
            } else {
                query = query.replace("[code]", "null");
            }
            query = query.replace("[use_count]", String.valueOf(resultSet.getLong(9)));
            query = query.replace("[updated]", "'" + String.valueOf(resultSet.getTimestamp(10)) + "'");

            concepts.add(query);
            i++;
            if (i % 10000 == 0) {
                LOG.info("Records added: " + i);
                System.gc();
            }
        }
        LOG.info("Total records added: " + concepts.size());
        //connection.close();
        resultSet.close();
        ps.close();
        return concepts;
    }

    private static String replaceInvalidChars(String value) {
        return value.replace("'", "''");
    }

    private static ArrayList<String> getConceptMap(String date, String server) throws Exception {

        ArrayList<String> conceptMap = new ArrayList<>();

        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();

        //String sql = "select legacy, core, updated, id from information_model.concept_map where deleted = 0 order by id asc;";
        String sql = "select legacy, core, updated, id from information_model.concept_map where deleted = 0 and updated > '" + date + "' order by legacy asc;";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        LOG.info("Fetching updated contents of concept_map table.");

        String query = "";
        int i = 0;
        while (resultSet.next()) {
            if (server.equalsIgnoreCase(SQL_SERVER)) {
                query = "update concept_map set legacy = [legacy], " +
                        "core = [core], " +
                        "updated = [updated] " +
                        "where id = [id] " +
                        "if @@ROWCOUNT = 0 " +
                        "insert into concept_map values ([legacy],[core],[updated],[id]);";
            } else {
                query = "INSERT INTO concept_map (`legacy`,`core`,`updated`,`id`) " +
                        "VALUES ([legacy],[core],[updated],[id]) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "`legacy` = [legacy]," +
                        "`core` = [core]," +
                        "`id` = [id]," +
                        "`updated` = [updated];";
            }

            query = query.replace("[legacy]", String.valueOf(resultSet.getLong(1)));
            query = query.replace("[core]", String.valueOf(resultSet.getLong(2)));
            query = query.replace("[updated]", "'" + String.valueOf(resultSet.getTimestamp(3)) + "'");
            query = query.replace("[id]", String.valueOf(resultSet.getInt(4)));

            conceptMap.add(query);
            i++;
            if (i % 10000 == 0) {
                LOG.info("Records added: " + i);
                System.gc();
            }
        }

        sql = "select id from information_model.concept_map where deleted = 1 order by legacy asc;";
        ps = connection.prepareStatement(sql);
        ps.executeQuery();
        resultSet = ps.getResultSet();
        LOG.info("Fetching marked as deleted contents of concept_map table.");

        i = 0;
        while (resultSet.next()) {
            query = "DELETE FROM concept_map WHERE id = [id];";
            query = query.replace("[id]", String.valueOf(resultSet.getInt(1)));
            conceptMap.add(query);
            i++;
            if (i % 10000 == 0) {
                LOG.info("Records added: " + i);
                System.gc();
            }
        }

        LOG.info("Total records added: " + conceptMap.size());
        //connection.close();
        resultSet.close();
        ps.close();
        return conceptMap;
    }

    private static ArrayList<String> getConceptProperty(String date) throws Exception {

        ArrayList<String> conceptProperty = new ArrayList<>();

        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();

        //TODO: For now always select all as we have no way to update concept_property_object table yet since it has no PRIMARY key
        //String sql = "select * from information_model.cpo_92842 where updated > '" + date + "' order by dbid asc;";
        String sql = "select * from information_model.cpo_92842;";

        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        LOG.info("Fetching contents of concept_property_object table.");

        String query = "";
        int i = 0;
        while (resultSet.next()) {
            /*
            query = "update concept_property_object set dbid = [dbid], " +
                    "group = [group]," +
                    "property = [property]," +
                    "value = [value]," +
                    "updated = [updated] " +
                    "where dbid = [dbid]  " +
                    "and group = [group]  " +
                    "and property = [property]  " +
                    "if @@ROWCOUNT = 0 " +
                    "insert into concept values ([dbid],[group],[property],[value],[updated]);";
             */
            query =  "insert into concept_property_object values ([dbid],[group],[property],[value],[updated]);";

            query = query.replace("[dbid]", String.valueOf(resultSet.getLong(1)));
            query = query.replace("[group]", String.valueOf(resultSet.getLong(2)));
            query = query.replace("[property]", String.valueOf(resultSet.getLong(3)));
            query = query.replace("[value]", String.valueOf(resultSet.getLong(4)));
            query = query.replace("[updated]", "'" + String.valueOf(resultSet.getTimestamp(5)) + "'");

            conceptProperty.add(query);
            i++;
            if (i % 10000 == 0) {
                LOG.info("Records added: " + i);
                System.gc();
            }
        }
        LOG.info("Total records added: " + conceptProperty.size());
        connection.close();
        resultSet.close();
        ps.close();
        return conceptProperty;
    }

    private static ZipFile zipAdhocFiles(File dataDir) throws Exception {
        File zip = new File(dataDir.getParentFile().getAbsolutePath() + File.separator + "concepts" + ".zip");
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