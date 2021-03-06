package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.cohortmanager.CohortManager;
import org.endeavourhealth.cohortmanager.models.CohortEntityDG;
import org.endeavourhealth.cohortmanager.querydocument.models.LibraryItem;
import org.endeavourhealth.scheduler.Main;
import org.endeavourhealth.scheduler.models.database.CohortResultsEntity;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.*;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BuildCohort implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(BuildCohort.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = null;
        try {
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
            for (ExtractEntity entity : extractsToProcess) {

                int extractId = entity.getExtractId();
                int cohortId = entity.getCohortId();
                boolean clearDownCohort = entity.getClearCohortEveryRun() != 0;

                try {
                    // clear previous extract cohort results
                    CohortResultsEntity cohortResultsEntity = new CohortResultsEntity();
                    if (clearDownCohort) {
                        cohortResultsEntity.clearCohortResults(extractId);
                    }

                    // get cohort query document for this extract
                    CohortEntityDG cohortEntity = new CohortEntityDG();
                    LibraryItem libraryItem = cohortEntity.getCohort(cohortId);

                    // run cohort query for this extract
                    CohortManager cohortManager = new CohortManager();
                    cohortManager.runCohort(libraryItem, extractId);
                }
                catch (Exception e){
                    LOG.error("Unknown error encountered in cohort runner. " + e.getMessage());
                }
                LOG.info("Extract ID: " + extractId);
            }
            Main.getInstance().setBuildCohortDone(true);
        } catch (SchedulerException e) {
            LOG.error("Unknown error encountered in cohort handling. " + e.getMessage());
        }
    }
}
