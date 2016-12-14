/**
 * 
 */
package com.ge.job.scheduler.quartz.jobs;

import com.ge.job.scheduler.services.GECrunchBaseCSVDownloader;
import com.ge.job.scheduler.services.GEProcedureCaller;
import com.ge.job.scheduler.services.GESolrExporter;
import com.ge.job.scheduler.util.AppLogger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.stereotype.Component;

import java.util.Calendar;

import static com.ge.job.scheduler.configuration.ConfigureQuartz.createCronTrigger;
import static com.ge.job.scheduler.configuration.ConfigureQuartz.createJobDetail;

@Component
@DisallowConcurrentExecution
public class GEDataLoaderJob implements Job {

	private final static AppLogger logger = AppLogger.getInstance();

	@Value("${cron.frequency}")
	private String frequency;

	@Autowired
	GECrunchBaseCSVDownloader crunchBaseCSVDownloader;

	@Autowired
	GEProcedureCaller procedureCaller;

	@Autowired
	GESolrExporter solrExporter;

	@Override
	public void execute(JobExecutionContext jobExecutionContext) {
		Calendar startDate = Calendar.getInstance();
		String jobName = jobExecutionContext.getJobDetail().getKey().getName();
		logger.info(">>>>>>>>>>>>>> Starting JOB (" + jobName + ") -> " + startDate.getTime() + " >>>>>>>>>>");
		logger.info("Running GEDataLoaderJob | frequency {}", frequency);

		try {
			crunchBaseCSVDownloader.downloadCsvAndLoadInputTables();
			procedureCaller.loadCrunchBaseDataToCompanyProfile();
			solrExporter.exportToSolr();
			logger.info(" GEDataLoaderJob : execute() : Successfully completed!!!");
		} catch (Exception e) {
			logger.error("Error : GEDataLoaderJob : execute() : Exception --> ", e);
		}

		Calendar endDate = Calendar.getInstance();
		logger.info(">>>>>>>>>>>>>> Ending JOB : Total TimeTaken to complete the Job --> "+
				(endDate.getTimeInMillis() - startDate.getTimeInMillis())+ " Secs >>>>>>>>>>");

	}

	@Bean(name = "jobWithCronTriggerBean")
	public JobDetailFactoryBean sampleJob() {
		return createJobDetail(this.getClass());
	}

	@Bean(name = "jobWithCronTriggerBeanTrigger")
	public CronTriggerFactoryBean sampleJobTrigger(@Qualifier("jobWithCronTriggerBean") JobDetail jobDetail) {
		return createCronTrigger(jobDetail, frequency);
	}
}
