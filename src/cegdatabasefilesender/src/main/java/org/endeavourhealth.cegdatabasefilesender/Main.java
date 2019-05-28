package org.endeavourhealth.cegdatabasefilesender;

import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.QueuedMessageDalI;
import org.endeavourhealth.core.database.rdbms.audit.models.RdbmsQueuedMessage;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.hibernate.internal.SessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.persistence.EntityManager;
import java.io.*;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.IOException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;


import org.apache.commons.io.FileUtils;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Main instance = null;

    private Main() {
    }

    public static Main getInstance()
    {
        if (instance == null)
            instance = new Main();

        return instance;
    }

    public static void main(String[] args) throws Exception {

        // ConfigManager.Initialize("ceg-database-file-sender");
        // Main main = Main.getInstance();

        LOG.info("Checking audit.queued_message table for stored zipped CSV files.");

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

            while (results.next()) {
                UUID queuedMessageId = UUID.fromString(results.getString("id"));
                getZipFilefromQueuedMessageTable(queuedMessageId);
            }

        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }

        LOG.info("Checking staging directory for zipped CSV files.");




    }

    private static void getZipFilefromQueuedMessageTable(UUID queuedMessageId) {
        QueuedMessageDalI queuedMessageDal = DalProvider.factoryQueuedMessageDal();
        try {
            String payload = queuedMessageDal.getById(queuedMessageId);
            byte [] bytes = Base64.getDecoder().decode(payload);
            writeZipFileToDirectory(bytes, queuedMessageId);
            // Once the file has been taken from audit.queued_message it can be deleted
            // queuedMessageDal.delete(queuedMessageId);

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private static void writeZipFileToDirectory(byte[] bytes, UUID queuedMessageId) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        File file = new File("C:/JDBC/Output/" +
                sdf.format(new Date()) + "_" +
                queuedMessageId.toString() +
                "_Subscriber_File.zip");
        try {
            FileUtils.writeByteArrayToFile(file, bytes);
            LOG.info("Written ZIP file to " + file);

        } catch (IOException ex) {
            LOG.error("Failed to write ZIP file to " + file, ex);
        }
    }

    private static void helperMethod() {

    }

    private static File[] getFilesFromDirectory(String directory, String prefix) {
        final String str = prefix;
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                return file.getName().startsWith(str);
            }
        };
        return new File(directory).listFiles(fileFilter);
    }
}
