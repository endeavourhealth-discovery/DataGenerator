package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleAliveJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduleAliveJob.class);

    public void execute(JobExecutionContext arg0) {
        LOG.debug("Scheduler still running....");
    }
}

