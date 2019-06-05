package org.endeavourhealth.filer.util;

import com.google.common.base.Strings;
import com.jcraft.jsch.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

public class SftpUtil {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpUtil.class);

    private ConnectionDetails connectionDetails;
    private JSch jSch;
    private Session session;
    private ChannelSftp channel;

    public SftpUtil(ConnectionDetails connectionDetails) {

        this.connectionDetails = connectionDetails;

        Validate.notEmpty(connectionDetails.getHostname(), "hostname is empty");
        Validate.notEmpty(connectionDetails.getUsername(), "username is empty");
        Validate.isTrue(connectionDetails.getPort() > 0, "port must be positive");
    }

    public void open() throws JSchException, IOException {

        //JSch.setLogger(new Logger());   //uncomment to enable JSch verbose SSH logging

        this.jSch = new JSch();

        String prvKey = this.connectionDetails.getClientPrivateKey().trim();
        String pw = this.connectionDetails.getClientPrivateKeyPassword().trim();
        if (!Strings.isNullOrEmpty(prvKey)) {
            jSch.addIdentity("client-private-key", prvKey.getBytes(), null, pw.getBytes());
        }

        //NOTE: To find the public host key, use SSH sftp to connect to the server and then copy the
        //record from the ~/.ssh/known_hosts file. It's easier to work out the correct record if the known_hosts
        //is first backed up, then emptied, then you know exactly which record is for the new server
        String hostPublicKey = this.connectionDetails.getHostPublicKey();
        if (StringUtils.isNotBlank(hostPublicKey)) {
            String knownHosts = this.connectionDetails.getKnownHostsString();
            jSch.setKnownHosts(new ByteArrayInputStream(knownHosts.getBytes()));
            this.session = jSch.getSession(this.connectionDetails.getUsername(),
                    this.connectionDetails.getHostname(),
                    this.connectionDetails.getPort());
        } else {
            this.session = jSch.getSession(this.connectionDetails.getUsername(),
                    this.connectionDetails.getHostname(),
                    this.connectionDetails.getPort());
            this.session.setConfig("StrictHostKeyChecking", "no");
        }

        // no private key supplied and using standard password authentication
        if (Strings.isNullOrEmpty(prvKey) && !Strings.isNullOrEmpty(pw)) {
            session.setPassword(pw);
        }

        this.session.connect();

        this.channel = (ChannelSftp)session.openChannel("sftp");
        this.channel.connect();
    }

    public static class Logger implements com.jcraft.jsch.Logger {
        static java.util.Hashtable name=new java.util.Hashtable();
        static{
            name.put(new Integer(DEBUG), "DEBUG: ");
            name.put(new Integer(INFO), "INFO: ");
            name.put(new Integer(WARN), "WARN: ");
            name.put(new Integer(ERROR), "ERROR: ");
            name.put(new Integer(FATAL), "FATAL: ");
        }
        public boolean isEnabled(int level){
            return true;
        }
        public void log(int level, String message){
            LOG.info(name.get(new Integer(level)).toString());
            LOG.info(message);
        }
    }

    public List<RemoteFile> getFileList(String remotePath) throws SftpException {

        //an error is raised if we try to list the files without first changing into the directory
        channel.cd(remotePath);

        //trying alternatives that work on all known SFTP servers
        Vector<ChannelSftp.LsEntry> fileList = channel.ls(".");
        //Vector<ChannelSftp.LsEntry> fileList = channel.ls("\\");

        return fileList
                .stream()
                .filter(t -> !t.getAttrs().isDir())
                .map(t ->
                        new RemoteFile(t.getFilename(),
                                t.getAttrs().getSize(),
                                LocalDateTime.ofInstant(new Date((long)t.getAttrs().getMTime() * 1000L).toInstant(), ZoneId.systemDefault())
                        )
                )
                .collect(Collectors.toList());
    }

    public InputStream getFile(String remotePath) throws SftpException {
        String name = FilenameUtils.getName(remotePath);
        return channel.get(name);
    }

    public InputStream getFile(String path, String file) throws SftpException {
        channel.cd(path);
        String name = FilenameUtils.getName(file);
        return channel.get(name);
    }

    public void deleteFile(String remotePath) throws SftpException {
        channel.rm(remotePath);
    }

    public void close() {
        if (channel != null && channel.isConnected())
            channel.disconnect();

        if (session != null && session.isConnected())
            session.disconnect();
    }

    public void put(String localPath, String destinationPath) throws SftpException {
        channel.put(localPath, destinationPath);
    }
}