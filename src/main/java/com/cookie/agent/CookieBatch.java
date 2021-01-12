package com.cookie.agent;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CookieBatch {

	private static final Logger logger = Logger.getLogger(CookieBatch.class);
	
	// Defines batch job
	public static enum BatchJob {
		EXPIRE_POINT,
		UPDATE_DEVICE_CURR_STATE,
		ACCUMULATE_DATA,
        SUMMARY_DATA,
        DELETE_DATA,
        IAMPORT_GET_DATA,
        IAMPORT_SYNC
    }
	
	private static CookieBatchManager manager;

	/**
     * Main method
     *
     * @param args 0 is retrieve start date, args 1 is retrieve end date
     */
	public static void main(String[] args) {
        PropertyConfigurator.configure(CookieBatch.class.getClassLoader().getResource("properties/log4j.properties"));
        
        logger.info("agent start");
        
        try {
            manager = new CookieBatchManager();
            manager.init();
            
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Seoul"));
            cal.setTime(new Date());
            
            Date currentDate = cal.getTime();
            Date startDate = null, endDate = null;
            
            executeBatchJob(BatchJob.EXPIRE_POINT, currentDate);
            executeBatchJob(BatchJob.UPDATE_DEVICE_CURR_STATE, currentDate);
            executeBatchJob(BatchJob.ACCUMULATE_DATA, currentDate);
            
            if (args.length >= 2) { // recollect data in specific period
                SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd");
                dateTimeFormat.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
                
                startDate = dateTimeFormat.parse(args[0]); //retrieve start date
                endDate = dateTimeFormat.parse(args[1]); //retrieve end date
            }
            
            executeBatchJob(BatchJob.SUMMARY_DATA, currentDate, startDate, endDate);
            executeBatchJob(BatchJob.DELETE_DATA, currentDate);
            executeBatchJob(BatchJob.IAMPORT_GET_DATA, currentDate);
            executeBatchJob(BatchJob.IAMPORT_SYNC, currentDate);
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
            //MailManager.getInstance().sendEmail("[COOKIE-BATCH] Error Message", e.getMessage());
        }

        logger.info("agent end");
    }
	
	/**
     * Execute batch job
     *
     * @param job				batch job
     * @param executeDateTime 	batch execution time
     */
	private static void executeBatchJob(BatchJob job, Date executeDateTime) throws Exception {
	    executeBatchJob(job, executeDateTime, null, null);
	}
	
	/**
     * Execute batch job
     *
     * @param job				batch job
     * @param executeDateTime 	batch execution time
     * @param startDate			retrieve start date
     * @param endDate			retrieve end date
     */
	private static void executeBatchJob(BatchJob job, Date executeDateTime, Date startDate, Date endDate) throws Exception {
	    logger.info(String.format("%s start", job.toString()));
	    
	    long start = System.currentTimeMillis();
	    
	    switch (job) {
	    case EXPIRE_POINT:
	    	manager.expirePoint(job.toString(), executeDateTime);
	    	break;
	    	
	    case UPDATE_DEVICE_CURR_STATE:
	    	manager.updateCurrentDeviceStatus(job.toString(), executeDateTime);
	    	break;
	    	
	    case ACCUMULATE_DATA:
	    	manager.accumulationData(job.toString(), executeDateTime);
	    	break;
	    
	    case SUMMARY_DATA:
	    	manager.summaryData(job.toString(), executeDateTime, startDate, endDate);
            break;
            
	    case DELETE_DATA:
	        manager.deleteData(job.toString(), executeDateTime);
	        break;
	        
	    case IAMPORT_GET_DATA:
	    	manager.getIamportData(job.toString(), executeDateTime);
	    	break;
	    	
	    case IAMPORT_SYNC:
	    	manager.syncPaymentData(job.toString(), executeDateTime);
	    	break;
	        
	    default:
	        break;
	    }
	    
	    logger.info(String.format("%s end (time taken: %d)", job.toString(), (System.currentTimeMillis() - start)));
	}
}
