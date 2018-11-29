package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BuildCohort implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(BuildCohort.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = null;
        try {
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
        } catch (SchedulerException e) {
            LOG.error("Unknown error encountered in cohort handling. " + e.getMessage());
        }


    }
}
