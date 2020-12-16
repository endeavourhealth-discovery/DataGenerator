package org.endeavourhealth.cegdatabasefilesender;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.endeavourhealth.scheduler.util.SftpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String MYSQL = "mysql";
    private static final String SQL_SERVER = "sql_server";

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 12) {
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
            LOG.info("Concepts Schema");
            LOG.info("Target Database Server");
            LOG.info("Target Schema");
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
        LOG.info("Concepts Schema        : " + args[8]);
        LOG.info("Target Database Server : " + args[9]);
        LOG.info("Target Schema          : " + args[10]);
        LOG.info("Delta Date             : " + args[11]);

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.applyPattern(args[11]);
        } catch (Exception e) {
            throw new Exception("Invalid date specified. " + args[9]);
        }

        if (args[9].equalsIgnoreCase("mysql") && args[9].equalsIgnoreCase("sql_server")) {
            throw new Exception("Invalid Target Database Server specified: " + args[9] + " . Only mysql or sql_server is allowed");
        }

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

        ArrayList<String> concept = getConcept(args[11], args[9]);
        createConceptFile(sourceDir, concept, args[8]);

        ArrayList<String> conceptMap = getConceptMap(args[11], args[9]);
        createConceptMapFile(sourceDir, conceptMap, args[8]);

        ArrayList<String> snomedToBnf = getSnomedToBNFData(args[11], args[9]);
        boolean hasSnomedToBnf = snomedToBnf.size() > 0;
        if (hasSnomedToBnf) {
            createSnomedToBNFFile(sourceDir, snomedToBnf, args[8]);
        }

        createSPFile(sourceDir, args[8], args[9], args[10], args[11], hasSnomedToBnf);

        ArrayList<File> zipFiles = new ArrayList<>();
        File zipFile = zipFiles(sourceDir).getFile();
        File cert = new File(args[7]);

        encryptFile(zipFile, cert);

        try {
            for (File file : sourceDir.getParentFile().listFiles()) {
                if (file.isFile() && file.getName().startsWith("concepts")) {
                    zipFiles.add(file);
                }
            }
            sftp.open();
            String location = args[5];
            LOG.info("Starting file upload.");
            for (File file : zipFiles) {
                sftp.put(file.getAbsolutePath(), location);
            }
            sftp.close();
            File archiveDir = new File(args[1]);
            if (!archiveDir.exists()) {
                FileUtils.forceMkdir(archiveDir);
            }
            for (File file : zipFiles) {
                File archive = new File(archiveDir.getAbsolutePath() + File.separator +
                        file.getName().substring(0, file.getName().length() - 4) + "_" + args[11] + "." +
                        FilenameUtils.getExtension(file.getName()));
                FileUtils.copyFile(file, archive);
            }
        } catch (Exception e) {
            LOG.error("Unable to do SFTP operation. " + e.getMessage());
            System.exit(-1);
        }

        for (File file : zipFiles) {
            file.deleteOnExit();
        }

        LOG.info("Process completed.");
        System.exit(0);
    }

    private static void createSnomedToBNFFile(File sourceDir, ArrayList<String> data, String schema) throws Exception {
        File file = new File(sourceDir.getAbsolutePath() + File.separator + "SnomedToBnf.sql");
        file.createNewFile();
        LOG.info("Generating snomed to bnf sql.");
        FileWriter writer = new FileWriter(file);
        writer.write("use " + schema + ";" + System.lineSeparator());
        for (String str : data) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static ArrayList<String> getSnomedToBNFData(String date, String server) throws Exception {

        ArrayList<String> data = new ArrayList<>();
        Connection connection = ConnectionManager.getReferenceNonPooledConnection();

        String sql = "select * from reference.snomed_to_bnf_chapter_lookup where dt_last_updated > '" + date + "' order by snomed_code asc;";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ResultSet resultSet = ps.getResultSet();
        LOG.info("Fetching contents of snomed_to_bnf_chapter_lookup table.");

        String query = "";
        int i = 0;
        while (resultSet.next()) {

            if (server.equalsIgnoreCase(SQL_SERVER)) {
                query = "update snomed_to_bnf_chapter_lookup " +
                        "set bnf_chapter_code = '[bnf_chapter_code]', " +
                        "dt_last_updated = '[dt_last_updated]' " +
                        "where snomed_code = '[snomed_code]' " +
                        "if @@ROWCOUNT = 0 " +
                        "insert into snomed_to_bnf_chapter_lookup values ('[snomed_code]','[bnf_chapter_code]','[dt_last_updated]');";
            } else {
                query = "INSERT INTO snomed_to_bnf_chapter_lookup (`snomed_code`,`bnf_chapter_code`,`dt_last_updated`) " +
                        "VALUES ('[snomed_code]','[bnf_chapter_code]','[dt_last_updated]') " +
                        "ON DUPLICATE KEY UPDATE " +
                        "`bnf_chapter_code` = '[bnf_chapter_code]'," +
                        "`dt_last_updated` = '[dt_last_updated]';";
            }

            query = query.replace("[snomed_code]", resultSet.getString(1));
            query = query.replace("[bnf_chapter_code]", resultSet.getString(2));
            query = query.replace("[dt_last_updated]", String.valueOf(resultSet.getTimestamp(3)));

            data.add(query);
            i++;
            if (i % 10000 == 0) {
                LOG.info("Records added: " + i);
                System.gc();
            }
        }
        LOG.info("Total records added: " + data.size());
        resultSet.close();
        ps.close();
        connection.close();
        return data;
    }

    private static void createConceptFile(File sourceDir, ArrayList<String> concept, String schema) throws Exception {
        File conceptFile = new File(sourceDir.getAbsolutePath() + File.separator + "concept.sql");
        conceptFile.createNewFile();
        LOG.info("Generating concept sql.");
        FileWriter writer = new FileWriter(conceptFile);
        writer.write("use " + schema + ";" + System.lineSeparator());
        for (String str : concept) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static void createConceptMapFile(File sourceDir, ArrayList<String> conceptMap, String schema) throws Exception {
        File conceptMapFile = new File(sourceDir.getAbsolutePath() + File.separator + "concept_map.sql");
        conceptMapFile.createNewFile();
        LOG.info("Generating concept_map sql.");
        FileWriter writer = new FileWriter(conceptMapFile);
        writer.write("use " + schema + ";" + System.lineSeparator());
        for (String str : conceptMap) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }

    private static void createConceptPropertyFile(File sourceDir, ArrayList<String> conceptProperty, String schema) throws Exception {
        File conceptMapFile = new File(sourceDir.getAbsolutePath() + File.separator + "concept_property_object.sql");
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

    private static void createConceptTctFile(File sourceDir, ArrayList<String> conceptTct, String schema) throws Exception {
        File conceptTctFile = new File(sourceDir.getAbsolutePath() + File.separator + "concept_tct.sql");
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

        Connection connection = ConnectionManager.getInformationModelNonPooledConnection();

        //String sql = "select * from information_model.concept order by dbid asc;";
        String sql = "select * from information_model.concept where updated > '" + date + "' order by updated asc;";
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

    private static ArrayList<String> getConceptMap(String date, String server) throws Exception {

        ArrayList<String> conceptMap = new ArrayList<>();

        Connection connection = ConnectionManager.getInformationModelNonPooledConnection();

        //String sql = "select legacy, core, updated, id from information_model.concept_map where deleted = 0 order by id asc;";
        String sql = "select legacy, core, updated, id, deleted from information_model.concept_map where updated > '" + date + "' order by id asc;";
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
                        "updated = [updated], " +
                        "deleted = [deleted] " +
                        "where id = [id] " +
                        "if @@ROWCOUNT = 0 " +
                        "insert into concept_map values ([legacy],[core],[updated],[id],[deleted]);";
            } else {
                query = "INSERT INTO concept_map (`legacy`,`core`,`updated`,`id`,`deleted`) " +
                        "VALUES ([legacy],[core],[updated],[id],[deleted]) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "`legacy` = [legacy]," +
                        "`core` = [core]," +
                        "`id` = [id]," +
                        "`deleted` = [deleted]," +
                        "`updated` = [updated];";
            }

            query = query.replace("[legacy]", String.valueOf(resultSet.getLong(1)));
            query = query.replace("[core]", String.valueOf(resultSet.getLong(2)));
            query = query.replace("[updated]", "'" + String.valueOf(resultSet.getTimestamp(3)) + "'");
            query = query.replace("[id]", String.valueOf(resultSet.getInt(4)));
            query = query.replace("[deleted]", String.valueOf(resultSet.getInt(5)));

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

        Connection connection = ConnectionManager.getInformationModelNonPooledConnection();

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

    private static ZipFile zipFiles(File dataDir) throws Exception {
        File zip = new File(dataDir.getParentFile().getAbsolutePath() + File.separator + "concepts" + ".zip");
        if (zip.exists()) {
            FileUtils.forceDelete(zip);
        }
        ZipFile zipFile = new ZipFile(zip);
        ZipParameters parameters = new ZipParameters();
        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_MAXIMUM);
        parameters.setIncludeRootFolder(false);
        zipFile.createZipFileFromFolder(dataDir, parameters, true, 10485760);
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
            throw ex;

        } catch (NoSuchProviderException ex) {
            LOG.error("Error encountered in certificate provider. " + ex.getMessage());
            throw ex;
        }
        return PgpEncryptDecrypt.encryptFile(file, certificate, "BC");
    }

    private static void createSPFile(File sourceDir, String conceptSchema, String server,
                                     String targetSchema, String date, boolean hasSnomedToBnf) throws Exception {

        File spFiles = new File(sourceDir.getParent() + File.separator + "stored_procedures");

        ArrayList<String> mysqlFilenames = new ArrayList();
        mysqlFilenames.add("update_core_concept_id_mysql.sql");
        mysqlFilenames.add("update_core_concept_tables_mysql.sql");
        if (hasSnomedToBnf) {
            mysqlFilenames.add("update_bnf_reference_mysql.sql");
            mysqlFilenames.add("update_bnf_tables_mysql.sql");
        }

        ArrayList<String> mssqlFilenames = new ArrayList();
        mssqlFilenames.add("update_core_concept_id_mssql.sql");
        mssqlFilenames.add("update_core_concept_tables_mssql.sql");
        if (hasSnomedToBnf) {
            mssqlFilenames.add("update_bnf_reference_mssql.sql");
            mssqlFilenames.add("update_bnf_tables_mssql.sql");
        }

        ArrayList<String> spNames = new ArrayList();
        spNames.add("update_core_concept_id");
        spNames.add("update_tables_with_core_concept_id");
        if (hasSnomedToBnf) {
            spNames.add("update_bnf_reference");
            spNames.add("update_tables_with_bnf");
        }

        ArrayList<String> execSPNames = new ArrayList();
        execSPNames.add("update_tables_with_core_concept_id");
        if (hasSnomedToBnf) {
            execSPNames.add("update_tables_with_bnf");
        }

        date = "'" + date + "'";
        File setupSPS = new File(sourceDir.getAbsolutePath() + File.separator + "concept_sps.setup");
        setupSPS.createNewFile();
        LOG.info("Generating concepts.sps file");
        FileWriter writer = new FileWriter(setupSPS);
        writer.write("USE " + conceptSchema + ";" + System.lineSeparator());
        for (String sp : spNames) {
            writer.write("DROP PROCEDURE IF EXISTS " + sp + ";" + System.lineSeparator());
        }

        if (server.equalsIgnoreCase(MYSQL)) {
            for (String file : mysqlFilenames) {
                writer.write("DELIMITER //" + System.lineSeparator());
                String contents = FileUtils.readFileToString(new File(spFiles + File.separator + file));
                contents = contents.replaceAll("(\r\n|\r|\n|\n\r)", " ");
                contents = contents.replaceAll("<target>", targetSchema);
                contents = contents.replaceAll("<last_updated_date>", date);
                writer.write(contents + System.lineSeparator());
                writer.write("// DELIMITER ;" + System.lineSeparator());
            }
        } else {
            for (String file : mssqlFilenames) {
                String contents = FileUtils.readFileToString(new File(spFiles + File.separator + file));
                contents = contents.replaceAll("(\r\n|\r|\n|\n\r)", " ");
                contents = contents.replaceAll("<target>", targetSchema);
                contents = contents.replaceAll("<last_updated_date>", date);
                writer.write(contents + System.lineSeparator());
            }
        }

        writer.close();

        File executeSPS = new File(sourceDir.getAbsolutePath() + File.separator + conceptSchema + ".execute");
        writer = new FileWriter(executeSPS);
        for (String sp : execSPNames) {
            writer.write(sp + System.lineSeparator());
        }
        writer.close();
    }

    private static String replaceInvalidChars(String value) {
        value = value.replace("\n", "").replace("\r", "");
        return value.replace("'", "''");
    }
}
