package org.endeavourhealth.scheduler.util;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.endeavourhealth.scheduler.job.EncryptFiles;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class ZipEncryptUpload {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ZipEncryptUpload.class);

    public static void main(String[] args) {

        if (args == null || args.length != 7) {
            LOG.error("Invalid parameters.");
            System.exit(-1);
        }

        LOG.info("");
        LOG.info("Running Zip, Encrypt and Upload process with the following parameters:");
        LOG.info("Source Directory  : " + args[0]);
        LOG.info("Staging Directory : " + args[1]);
        LOG.info("Hostname          : " + args[2]);
        LOG.info("Port              : " + args[3]);
        LOG.info("Username          : " + args[4]);
        LOG.info("SFTP Location     : " + args[5]);
        LOG.info("Key File          : " + args[6]);
        LOG.info("");

        File source_dir = new File(args[0]);
        if (!source_dir.isDirectory() || !source_dir.exists() || source_dir.listFiles().length == 0) {
            LOG.info("");
            LOG.error("Source directory is empty.");
            LOG.info("");
            System.exit(-1);
        }

        File staging_dir = new File(args[1]);
        if (!staging_dir.exists()) {
            staging_dir.mkdirs();
        } else {
            File[] files = staging_dir.listFiles();
            if (files.length > 0) {
                LOG.info("");
                LOG.info("Staging directory is not empty.");
                for (File file : files) {
                    LOG.info("Deleting the file: " + file.getName());
                    file.delete();
                }
                LOG.info("");
            }
        }

        ConnectionDetails con = new ConnectionDetails();
        con.setHostname(args[2]);
        con.setPort(Integer.valueOf(args[3]));
        con.setUsername(args[4]);
        try {
            con.setClientPrivateKey(FileUtils.readFileToString(new File(args[6]), (String) null));
            con.setClientPrivateKeyPassword("");
        } catch (IOException e) {
            LOG.info("");
            LOG.error("Unable to read client private key file." + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        SftpConnection sftp = new SftpConnection(con);
        try {
            sftp.open();
            LOG.info("");
            LOG.info("SFTP connection established");
            LOG.info("");
            sftp.close();
        } catch (Exception e) {
            LOG.info("");
            LOG.error("Unable to connect to the SFTP server. " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        try {
            ZipEncryptUpload.zipDirectory(source_dir, staging_dir);

        } catch (Exception e) {
            LOG.info("");
            LOG.error("Unable to create the zip file/s." + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        try {
            File zipFile = new File(staging_dir.getAbsolutePath() +
                    File.separator +
                    source_dir.getName() +
                    ".zip");
            if (!ZipEncryptUpload.encryptFile(zipFile)) {
                LOG.info("");
                LOG.error("Unable to encrypt the zip file/s. ");
                LOG.info("");
                System.exit(-1);
            }
        } catch (Exception e) {
            LOG.info("");
            LOG.error("Unable to encrypt the zip file/s. " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        try {
            sftp.open();
            String location = args[5];
            File[] files = staging_dir.listFiles();
            LOG.info("");
            LOG.info("Starting file/s upload.");
            for (File file : files) {
                LOG.info("Uploading file:" + file.getName());
                sftp.put(file.getAbsolutePath(), location);
            }
            LOG.info("");
            sftp.close();
        } catch (Exception e) {
            LOG.info("");
            LOG.error("Unable to do SFTP operation. " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }
        LOG.info("");
        LOG.info("Process completed.");
        LOG.info("");
        System.exit(0);
    }

    public static void zipDirectory(File source, File staging) throws Exception {

        LOG.info("");
        LOG.info("Compressing contents of: " + source.getAbsolutePath());

        ZipFile zipFile = new ZipFile(staging + File.separator + source.getName() + ".zip");
        LOG.info("Creating file: " + zipFile.getFile().getAbsolutePath());

        // Set the zip file parameters
        ZipParameters parameters = new ZipParameters();
        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
        parameters.setIncludeRootFolder(false);

        // Create the multi-part zip file from the files in the
        // specified folder, using the zip file parameters
        zipFile.createZipFileFromFolder(source, parameters, true, 10485760);

        LOG.info(staging.listFiles().length + " File/s successfully created.");
        LOG.info("");
    }

    public static boolean encryptFile(File file) throws Exception {

        X509Certificate certificate = null;
        try {
            Security.addProvider(new BouncyCastleProvider());
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509", "BC");
            certificate =
                    (X509Certificate) certFactory.generateCertificate(
                            EncryptFiles.class.getClassLoader().getResourceAsStream("discovery.cer"));
        } catch (CertificateException e) {
            LOG.error("Error encountered in certificate generation. " + e.getMessage());
            throw e;
        } catch (NoSuchProviderException e) {
            LOG.error("Error encountered in certificate provider. " + e.getMessage());
            throw e;
        }

        LOG.info("Encrypting the file: " + file.getAbsolutePath());
        return PgpEncryptDecrypt.encryptFile(file, certificate, "BC");
    }
}
