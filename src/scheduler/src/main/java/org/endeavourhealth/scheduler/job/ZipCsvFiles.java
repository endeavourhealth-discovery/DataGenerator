package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(TransferEncryptedFilesToSftp.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Zipping CSV files");
        LOG.info("Zipping CSV files");
    }
}
