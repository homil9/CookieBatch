package com.cookie.agent;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.cookie.agent.util.HttpUtils;
import net.sf.json.JSONNull;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.log4j.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;

import com.cookie.agent.CookieBatch.BatchJob;
import com.cookie.agent.dao.CommonDAO;
import com.cookie.agent.dao.SummaryDAO;
import com.cookie.agent.model.AgentHistory;
import com.cookie.agent.model.DatePeriod;
import com.cookie.agent.model.PaymentInfo;
import com.cookie.agent.util.ConfigurationManager;
import com.cookie.agent.util.IMPUtils;
import com.cookie.agent.util.MyBatisConnectionFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class CookieBatchManager {
    
    private static final Logger logger = Logger.getLogger(CookieBatchManager.class);
    private static final String datePattern = "yyyy-MM-dd";
    private static final String dateTimePattern = "yyyy-MM-dd HH:mm:ss";
    
    // Defines summary type
    public static enum SummaryType {
        DAILY,
        WEEKLY,
        MONTHLY
    }
    
    private CommonDAO commonDao;
    private SummaryDAO summaryDao;
    
    private SimpleDateFormat dateFormat;
    private SimpleDateFormat dateTimeFormat;
    
    private Map<String, AgentHistory> agentInfoMap;
    private HashMap<String, String> impInfoMap;
    
    private int agentNo;

    /**
     * CookieBatchManager Constructor
     */
    public CookieBatchManager() {
        SqlSessionFactory sqlSessionFactory = MyBatisConnectionFactory.getSqlSessionFactory();
        
        commonDao = new CommonDAO(sqlSessionFactory);
        summaryDao = new SummaryDAO(sqlSessionFactory);
        
        dateFormat = new SimpleDateFormat(datePattern);
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
        
        dateTimeFormat = new SimpleDateFormat(dateTimePattern);
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
    }
    
    /**
     * Initialize
     */
    public void init() {
    	agentNo = ConfigurationManager.getConfiguration().getInt("agent_no");
    	
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	parameters.put("agentNo", agentNo);
    	
    	agentInfoMap = new HashMap<String, AgentHistory>();
    	
    	List<AgentHistory> agentHistories = commonDao.selectAgentHistory(parameters);
    	for (AgentHistory history : agentHistories) {
    		agentInfoMap.put(history.getJobType(), history);
    	}
    	
    	impInfoMap = new HashMap<String, String>();
        List<HashMap> mapList = commonDao.selectIamportInfo();
        for (HashMap map : mapList) {
        	impInfoMap.put(map.get("keyName").toString(), map.get("value").toString());
        }
    }
    
    /**
     * Update Expired/Expiring Point
     *
     * @param jobName			job name
     * @param executeDateTime	batch execution time
     */
    public void expirePoint(String jobName, Date executeDateTime) {
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	String errorMessage = null;
    	
    	try {
    		commonDao.updateExpiredPoint();
    		
    		List<Map<String, Object>> list = commonDao.selectExpiringPoint();
    		if (list != null && list.size() > 0) {
    			for (Map<String, Object> map : list) {
    				commonDao.updateExpiringPoint(map);
    			}
    		}
    	} catch (Exception e) {
    		errorMessage = ExceptionUtils.getStackTrace(e);
    		logger.error(errorMessage);
    	}
    	
    	// insert agent history
    	parameters.put("agentNo", agentNo);
    	parameters.put("startDate", dateTimeFormat.format(executeDateTime));
    	parameters.put("jobType", jobName);
    	parameters.put("errorMessage", errorMessage);
    	
    	commonDao.insertAgentHistory(parameters);
    }
    
    /**
     * Delete database data
     *
     * @param jobName			job name
     * @param executeDateTime	batch execution time
     */
    public void deleteData(String jobName, Date executeDateTime) {
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	String errorMessage = null;
    	
    	try {
    		commonDao.updateDormantCustomer(); //휴면 계정 처리 (마지막 로그인한 후 1년 지나면 휴면 계정으로 전환)
    		commonDao.deleteCustomer(); // 준회원, 탈퇴회원, 휴면 계정의 경우 6달 지난 후 삭제 처리
    		
    		/*
            //1년 지난 데이터는 삭제?
            commonDao.deleteCashPointHistory();
            commonDao.deleteBikeUseHistory();
            commonDao.deleteVocData();
            */
    	} catch (Exception e) {
    		errorMessage = ExceptionUtils.getStackTrace(e);
    		logger.error(errorMessage);
    	}
    	
    	// insert agent history
    	parameters.put("agentNo", agentNo);
    	parameters.put("startDate", dateTimeFormat.format(executeDateTime));
    	parameters.put("jobType", jobName);
    	parameters.put("errorMessage", errorMessage);
    	
    	commonDao.insertAgentHistory(parameters);
    }
    
    /**
     * Get payment data from iamport
     *
     * @param jobName			job name
     * @param executeDateTime	batch execution time
     */
    public void getIamportData(String jobName, Date executeDateTime) {
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	String errorMessage = null;
    	
    	int lastTo = 0;
    	
    	try {
    		// get last agent history
    		AgentHistory info = agentInfoMap.get(jobName);
    		if (info != null && !StringUtils.isEmpty(info.getJobInfo())) {
    			lastTo = Integer.parseInt(info.getJobInfo());
    		}
    		
    		// get iamport token
    		String token = IMPUtils.getToken(impInfoMap.get("IMP_API_KEY"), impInfoMap.get("IMP_API_SECRET_KEY"));
    		String status = "all"; // status : all,ready,paid,cancelled,failed
    		String includeStatus = status.equals("all") ? "paid|cancelled" : null;
    		
        	int page = 1;
        	int limit = 100;
        	int from = lastTo + 1;
        	int to = (int)Math.floor((executeDateTime.getTime() - 3600000) / 1000); // before 1 hour
        	int total;
        	
        	//if (to - from < 3600) return; // don't re-execute in 1 hour
        	
        	while (page > 0) {
        		// get payment history list from iamport
            	String result = IMPUtils.getPaymentHistoryList(token, status, page, limit, from, to);
            	
            	// parse payment data
            	HashMap<String, Object> resultMap = parsePaymentData(result, includeStatus);
            	total = (Integer)resultMap.get("total");
            	
            	logger.info(String.format("%d - %d, total = %d, page = %d", from, to, total, page));
            	
            	@SuppressWarnings("unchecked")
				List<PaymentInfo> list = (List<PaymentInfo>)resultMap.get("list");
            	if (list.size() > 0) {
            		//insert payment history
                	commonDao.insertIMPPaymentHistory(resultMap);
            	}
            	
            	page = (Integer)resultMap.get("next");
        	}
        	
        	lastTo = to;
    	} catch (Exception e) {
    		errorMessage = ExceptionUtils.getStackTrace(e);
    		logger.error(errorMessage);
    	}
    	
    	// insert agent history
    	parameters.put("agentNo", agentNo);
    	parameters.put("startDate", dateTimeFormat.format(executeDateTime));
    	parameters.put("jobType", jobName);
    	if (lastTo > 0) {
    		parameters.put("jobInfo", lastTo);
    	}    	
    	parameters.put("errorMessage", errorMessage);
    	
    	commonDao.insertAgentHistory(parameters);
    }
    
    /**
     * Synchronizes data with iamport payment history
     *
     * @param jobName			job name
     * @param executeDateTime	batch execution time
     */
    public void syncPaymentData(String jobName, Date executeDateTime) {
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	String errorMessage = null;
    	
    	int from = 0, to = 0, lastTo = 0;
    	
    	try {
    		// get last agent history
    		AgentHistory info = agentInfoMap.get(jobName);
    		if (info != null && !StringUtils.isEmpty(info.getJobInfo())) {
    			from = Integer.parseInt(info.getJobInfo());
    		}
    		
    		if (from == 0) {
    			from = (int)Math.floor((executeDateTime.getTime() - 7200000) / 1000); // before 2 hour
    		}
    		
    		// get job information from last agent history
    		info = agentInfoMap.get(BatchJob.IAMPORT_GET_DATA.toString());
    		if (info != null && !StringUtils.isEmpty(info.getJobInfo())) {
    			to = Integer.parseInt(info.getJobInfo());
    		}
    		
    		if (to == 0) {
    			logger.info("to is 0");
    			return;
    		}
    		
    		// sync start/end date
    		Date syncStartDate = (from > 0) ? new Date(from * 1000L) : new Date();
            Date syncEndDate = new Date(to * 1000L);
            
            logger.info(String.format("%s - %s", dateTimeFormat.format(syncStartDate), dateTimeFormat.format(syncEndDate)));
            
            if (syncEndDate.compareTo(syncStartDate) < 0) {
            	throw new RuntimeException("syncStartDate/syncEndDate is invalid");
            }
            
            // processing cancelled payment data
            parameters.put("startDate", dateTimeFormat.format(syncStartDate));
        	parameters.put("endDate", dateTimeFormat.format(syncEndDate));
        	parameters.put("status", "cancelled");
        	
        	int point, remainPoint, remainCash, amount, cancelAmount;
        	
        	// get cancelled payment data for synchronization (different data compared to cookie payment)
        	List<HashMap> paymentMapList = commonDao.selectIMPPaymentHistory(parameters);
        	for (HashMap<String, Object> paymentMap : paymentMapList) {
        		if (paymentMap.get("paymentNo") == null) continue;
        		
        		// re-check payment status
        		PaymentInfo paymentInfo = commonDao.selectPaymentInfo(paymentMap);
    			if (paymentInfo.getStatus().equals("cancelled")) continue;
    			
    			point = Integer.parseInt(paymentMap.get("point").toString());
    			remainPoint = Integer.parseInt(paymentMap.get("remainPoint").toString());
    			remainCash = Integer.parseInt(paymentMap.get("remainCash").toString());
    			amount = Integer.parseInt(paymentMap.get("amount").toString());
    			cancelAmount = Integer.parseInt(paymentMap.get("cancelAmount").toString());
    			
    			paymentMap.put("cash", cancelAmount * -1);
    			paymentMap.put("point", point * -1);
    			paymentMap.put("remainPoint", remainPoint + (point * -1));
    			
    			if ("Y".equals(paymentMap.get("depositYn").toString())) {
                    paymentMap.put("remainCash", remainCash);
                    paymentMap.put("deposit", amount + (cancelAmount * -1));
                } else {
                    paymentMap.put("remainCash", remainCash + (cancelAmount * -1));
                }
    			// update payment, cash/point information
    			commonDao.updatePaymentInfo(paymentMap);
    			
    			logger.info(String.format("paymentMap = %s", paymentMap));
        	}

			// get wait payment data for synchronization (different data compared to cookie payment)
			List<HashMap> paymentMapList2 = commonDao.selectIMPPaymentHistory2(parameters);
			logger.info(String.format("sync wait imp paymentMapList2 = %s", paymentMapList2.size()));
			logger.info(String.format("sync wait imp paymentMapList2toString = %s", paymentMapList2.toString()));

			for (HashMap<String, Object> paymentMap : paymentMapList2) {
				if (paymentMap.get("paymentNo") == null) continue;

				// re-check payment status
				PaymentInfo paymentInfo = commonDao.selectPaymentInfo(paymentMap);
				if ("paid".equals(paymentInfo.getStatus())) continue;

				// get payment_imp_data
				if (JSONNull.getInstance().equals(paymentMap.get("paymentCode"))) {
					logger.info(String.format("sync wait paymentCode null = %s"));
					continue;
				}

				if (JSONNull.getInstance().equals(paymentMap.get("paymentOrderCode"))) {
					logger.info(String.format("sync wait paymentOrderCode null = %s"));
					continue;
				}

				//TransactionManager transaction = null;
				String impUid = String.valueOf(paymentMap.get("paymentCode"));
				String merchantUid = String.valueOf(paymentMap.get("paymentOrderCode"));

				//impService.updatePayment(impUid, merchantUid);
				// /common/payment/notification post
				String url = String.format("https://api.cookiebike.co.kr/app/common/payment/notification");

				Map<String, Object> headers = new HashMap<String, Object>();
				headers.put("Content-Type", "application/json;charset=UTF-8");

				JSONObject obj = new JSONObject();
				obj.put("imp_uid", impUid);
				obj.put("merchant_uid", merchantUid);
				obj.put("status", "paid");

				String responseData = HttpUtils.request(url, "POST", headers, obj.toString());
				JSONObject result = JSONObject.fromObject(JSONSerializer.toJSON(responseData));

				logger.info(String.format("sync wait imp paymentMap = %s", paymentMap));
				logger.info(String.format("sync wait cookie paymentInfo = %s", paymentInfo));
				logger.info(String.format("sync wait result = %s", result));
			}

        	lastTo = to;
    	} catch (Exception e) {
    		errorMessage = ExceptionUtils.getStackTrace(e);
    		logger.error(errorMessage);
    	}
    	
    	// insert agent history
    	parameters.put("agentNo", agentNo);
    	parameters.put("startDate", dateTimeFormat.format(executeDateTime));
    	parameters.put("jobType", jobName);
    	if (lastTo > 0) {
    		parameters.put("jobInfo", lastTo);
    	}
    	parameters.put("errorMessage", errorMessage);
    	
    	commonDao.insertAgentHistory(parameters);
    }
    
    /**
     * Update current device status
     *
     * @param jobName			job name
     * @param executeDateTime	batch execution time
     */
    public void updateCurrentDeviceStatus(String jobName, Date executeDateTime) {
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	String errorMessage = null;
    	
    	try {
    		summaryDao.updateCurrentDeviceStatus();
    	} catch (Exception e) {
    		errorMessage = ExceptionUtils.getStackTrace(e);
    		logger.error(errorMessage);
    	}
    	
    	// insert agent history
    	parameters.put("agentNo", agentNo);
    	parameters.put("startDate", dateTimeFormat.format(executeDateTime));
    	parameters.put("jobType", jobName);
    	parameters.put("errorMessage", errorMessage);
    	
    	commonDao.insertAgentHistory(parameters);
    }
    
    /**
     * Accumulation Data
     *
     * @param jobName			job name
     * @param executeDateTime	batch execution time
     */
    public void accumulationData(String jobName, Date executeDateTime) {
    	Map<String, Object> parameters = new HashMap<String, Object>();
    	int startBikeUseHistoryNo = 0, endBikeUseHistoryNo = 0;
    	int startCashPointHistoryNo = 0, endCashPointHistoryNo = 0;
    	int startVocProcessHistoryNo = 0, endVocProcessHistoryNo = 0;
    	String errorMessage = null;
    	
    	try {
    		// get last processing history no
    		AgentHistory info = agentInfoMap.get(jobName);
    		if (info != null && !StringUtils.isEmpty(info.getJobInfo())) {
    			String[] temps = info.getJobInfo().split(",");
    			
    			startBikeUseHistoryNo = Integer.parseInt(temps[0]) + 1;
    			startCashPointHistoryNo = Integer.parseInt(temps[1]) + 1;
    			startVocProcessHistoryNo = Integer.parseInt(temps[2]) + 1;
    		}
    		
    		// get current max history no
    		endBikeUseHistoryNo = summaryDao.selectBikeUseMaxHistoryNo();
    		endCashPointHistoryNo = summaryDao.selectCashPointMaxHistoryNo();
            
        	// accumulation data
    		parameters.put("startBikeUseHistoryNo", startBikeUseHistoryNo);
    		parameters.put("endBikeUseHistoryNo", endBikeUseHistoryNo);
    		parameters.put("startCashPointHistoryNo", startCashPointHistoryNo);
    		parameters.put("endCashPointHistoryNo", endCashPointHistoryNo);
    		
    		if (info != null && !StringUtils.isEmpty(info.getStartDate())) {
    			parameters.put("updateStartDate", info.getStartDate());
        		parameters.put("updateEndDate", dateTimeFormat.format(executeDateTime));
    		}
    		
        	summaryDao.accumulationData(parameters);
        	
        	// update customer detail
        	endVocProcessHistoryNo = summaryDao.selectVocProcessMaxHistoryNo();
        	parameters.put("startVocProcessHistoryNo", startVocProcessHistoryNo);
    		parameters.put("endVocProcessHistoryNo", endVocProcessHistoryNo);
    		
    		summaryDao.updateCustomerDetailData(parameters);
    		
    		// update daily payment summary data
    		DatePeriod period = summaryDao.selectUpdatePaymentPeriod(parameters);
    		if (period != null && period.getStartDate() != null && period.getEndDate() != null) {
    			parameters.put("startDate", period.getStartDate());
        		parameters.put("endDate", period.getEndDate());
        		summaryDao.summaryPaymentHistoryData(parameters, SummaryType.DAILY, true);
    		}
    	} catch (Exception e) {
    		errorMessage = ExceptionUtils.getStackTrace(e);
    		logger.error(errorMessage);
    	}
    	
    	// insert agent history
    	parameters.put("agentNo", agentNo);
    	parameters.put("startDate", dateTimeFormat.format(executeDateTime));
    	parameters.put("jobType", jobName);
    	parameters.put("jobInfo", "" + endBikeUseHistoryNo + "," + endCashPointHistoryNo + "," + endVocProcessHistoryNo);
    	parameters.put("errorMessage", errorMessage);
    	
    	commonDao.insertAgentHistory(parameters);
    }
    
    /**
     * Summary statistics data
     *
     * @param jobName			job name
     * @param executeDateTime	batch execution time
     */
    public void summaryData(String jobName, Date executeDateTime, Date startDate, Date endDate) throws Exception {
        if (executeDateTime == null) {
        	throw new RuntimeException("executeDateTime is null");
        }
        
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Seoul"));
        cal.setTime(executeDateTime);
        
        Date summaryStartDate = startDate;
        Date summaryEndDate = endDate;
        
        if (summaryEndDate == null) {
        	summaryEndDate = DateUtils.truncate(cal, Calendar.DATE).getTime();
        }
        
        if (summaryStartDate == null) {
        	cal.add(Calendar.DAY_OF_MONTH, -1);    //1 day ago
        	summaryStartDate = DateUtils.truncate(cal, Calendar.DATE).getTime();
        }
        
        logger.info(String.format("%s - %s", dateFormat.format(summaryStartDate), dateFormat.format(summaryEndDate)));
        
        if (summaryEndDate.compareTo(summaryStartDate) < 0) {
        	throw new RuntimeException("summaryStartDate/summaryEndDate is invalid");
        }
        
        Map<String, Object> parameters = new HashMap<String, Object>();
        String errorMessage = null;
        
        try {
        	long diff = executeDateTime.getTime() - summaryStartDate.getTime();
            long days = TimeUnit.MILLISECONDS.toDays(diff);
            
            // summary daily data
        	parameters.put("startDate", dateFormat.format(summaryStartDate));
        	parameters.put("endDate", dateFormat.format(summaryEndDate));
            summaryDao.summaryCustomerData(parameters, SummaryType.DAILY, (days < 2 /*30*/));
            summaryDao.summaryPaymentHistoryData(parameters, SummaryType.DAILY, (days < 2 /*30*/));
            summaryDao.summaryVocHistoryData(parameters, SummaryType.DAILY);
            
            // get last summary date
            AgentHistory info = agentInfoMap.get(jobName);
            if (info != null && !StringUtils.isEmpty(info.getStartDate())) {
    			parameters.put("updateStartDate", info.getStartDate());
    		} else {
    			parameters.put("updateStartDate", dateFormat.format(summaryStartDate));
    		}
            String bikeHistoryStartDateStr = summaryDao.selectBikeHistoryStartDate(parameters);
            
            // summary 기준 시작일보다 이전 데이터가 업데이트된 경우 업데이트된 일자부터 수집
            if (bikeHistoryStartDateStr != null && summaryStartDate.after(dateFormat.parse(bikeHistoryStartDateStr))) {
            	parameters.put("startDate", bikeHistoryStartDateStr);
            }
            summaryDao.summaryBikeHistoryData(parameters, SummaryType.DAILY);
            
            parameters.put("updateStartDate", dateFormat.format(executeDateTime));
            
            // get start date for customer data summary
            String summaryStartDateStr = summaryDao.selectCustomerDailySummaryStartDate(parameters);
            if (!StringUtils.isEmpty(summaryStartDateStr)) {
            	summaryStartDate = dateFormat.parse(summaryStartDateStr);
            	
            	// summary weekly data - customer data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_WEEK) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryCustomerData(parameters, SummaryType.WEEKLY, true);
                    
                    cal.add(Calendar.DAY_OF_MONTH, 7);
                }
                
                // summary monthly data - customer data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_MONTH) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryCustomerData(parameters, SummaryType.MONTHLY, true);
                    
                    cal.add(Calendar.MONTH, 1);
                }
            }
            
            // get start date for bike history data summary
            summaryStartDateStr = summaryDao.selectBikeDailySummaryStartDate(parameters);
            if (!StringUtils.isEmpty(summaryStartDateStr)) {
            	summaryStartDate = dateFormat.parse(summaryStartDateStr);
            	
            	// summary weekly data - bike history data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_WEEK) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryBikeHistoryData(parameters, SummaryType.WEEKLY);
                    
                    cal.add(Calendar.DAY_OF_MONTH, 7);
                }
                
                // summary monthly data - bike history data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_MONTH) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryBikeHistoryData(parameters, SummaryType.MONTHLY);
                    
                    cal.add(Calendar.MONTH, 1);
                }
            }
            
            // get start date for payment history data summary
            summaryStartDateStr = summaryDao.selectPaymentDailySummaryStartDate(parameters);
            if (!StringUtils.isEmpty(summaryStartDateStr)) {
            	summaryStartDate = dateFormat.parse(summaryStartDateStr);
            	
            	// summary weekly data - payment history data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_WEEK) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryPaymentHistoryData(parameters, SummaryType.WEEKLY, true);
                    
                    cal.add(Calendar.DAY_OF_MONTH, 7);
                }
                
                // summary monthly data - payment history data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_MONTH) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryPaymentHistoryData(parameters, SummaryType.MONTHLY, true);
                    
                    cal.add(Calendar.MONTH, 1);
                }
            }
            
            // get start date for voc data summary
            summaryStartDateStr = summaryDao.selectVocDailySummaryStartDate(parameters);
            if (!StringUtils.isEmpty(summaryStartDateStr)) {
            	summaryStartDate = dateFormat.parse(summaryStartDateStr);
            	
            	// summary weekly data - voc history data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_WEEK) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryVocHistoryData(parameters, SummaryType.WEEKLY);
                    
                    cal.add(Calendar.DAY_OF_MONTH, 7);
                }
                
                // summary monthly data - voc history data
                cal.setTime(summaryStartDate);
                cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_MONTH) - 1));
                
                while (summaryEndDate.compareTo(cal.getTime()) >= 0) {
                	parameters.put("startDate", dateFormat.format(cal.getTime()));
                    summaryDao.summaryVocHistoryData(parameters, SummaryType.MONTHLY);
                    
                    cal.add(Calendar.MONTH, 1);
                }
            }
        } catch (Exception e) {
    		errorMessage = ExceptionUtils.getStackTrace(e);
    		logger.error(errorMessage);
    	}
    	
        // insert agent history
    	parameters.put("agentNo", agentNo);
    	parameters.put("startDate", dateTimeFormat.format(executeDateTime));
    	parameters.put("jobType", jobName);
    	parameters.put("errorMessage", errorMessage);
    	
    	commonDao.insertAgentHistory(parameters);
    }
    
    /**
     * Parse iamport payment data
     *
     * @param data			payment json data
     * @param includeStatus	included payment status in payment data
     * @return HashMap		payment data
     */
    private HashMap<String, Object> parsePaymentData(String data, String includeStatus) {
    	HashMap<String, Object> resultMap = new HashMap<String, Object>();
    	List<PaymentInfo> paymentList = new ArrayList<PaymentInfo>();
    	
    	JSONObject result = JSONObject.fromObject(JSONSerializer.toJSON(data));
    	JSONObject response = result.getJSONObject("response");
    	JSONArray list = response.getJSONArray("list");
    	
    	String impUid;
    	String merchantUid;
    	String applyNum;
    	String status;
    	String pgProvider;
    	String pgMethod;
    	String buyerName;
    	int amount;
    	int paidAt;
    	int totalCancelledAmount = 0;
    	int cancelledAt;
    	
    	JSONArray cancelHistoryList;
    	
    	for (int i = 0; i < list.size(); i++) {
    		JSONObject obj = list.getJSONObject(i);
    		
    		impUid = obj.getString("imp_uid");
    		merchantUid = obj.getString("merchant_uid");
    		applyNum = obj.getString("apply_num");
    		status = obj.getString("status");
    		pgProvider = obj.getString("pg_provider");
    		pgMethod = obj.getString("pay_method");
            buyerName = obj.getString("buyer_name");
    		amount = obj.getInt("amount");
    		totalCancelledAmount = obj.getInt("cancel_amount");
    		paidAt = obj.getInt("paid_at");
    		
    		// includeStatus와 매칭되지 않는 status를 가진 payment정보는 skip
    		if (includeStatus != null && status.matches(includeStatus) == false) {
    			continue;
    		}
    		
    		if (status.equals("paid")) {
    			PaymentInfo payment = new PaymentInfo();
        		payment.setImpUid(impUid);
        		payment.setMerchantUid(merchantUid);
        		payment.setApplyNum(applyNum);
        		payment.setStatus(status);
        		payment.setPgProvider(pgProvider);
        		payment.setPgMethod(pgMethod);
        		payment.setBuyerName(buyerName);
        		payment.setPaidAt(dateTimeFormat.format(new Date((long)paidAt * 1000)));
        		payment.setAmount(amount);
        		payment.setTotalCancelledAmount(totalCancelledAmount);
        		payment.setCancelledAmount(0);
        		
        		paymentList.add(payment);
    		} else if (status.equals("cancelled")) {
    			cancelHistoryList = obj.getJSONArray("cancel_history");
    			/* 부분취소가 여러건일때 문제 생길 수 있음
        		for (int j = 0; j < cancelHistoryList.size(); j++) {
        			JSONObject historyObj = cancelHistoryList.getJSONObject(j);
        			
        			PaymentInfo payment = new PaymentInfo();
            		payment.setImpUid(impUid);
            		payment.setMerchantUid(merchantUid);
            		payment.setApplyNum(applyNum);
            		payment.setStatus(status);
            		payment.setAmount(amount);
            		payment.setTotalCancelledAmount(totalCancelledAmount);
            		payment.setCancelledAmount(historyObj.getInt("amount"));
            		
            		cancelledAt = historyObj.getInt("cancelled_at");
            		payment.setCancelledAt(dateTimeFormat.format(new Date((long)cancelledAt * 1000)));
            		
            		paymentList.add(payment);
        		}*/
    			
    			// 부분 취소시 1번째 건만 처리
        		if (cancelHistoryList != null && cancelHistoryList.size() > 0) {
                    JSONObject historyObj = cancelHistoryList.getJSONObject(0);
            		
            		PaymentInfo payment = new PaymentInfo();
            		payment.setImpUid(impUid);
            		payment.setMerchantUid(merchantUid);
            		payment.setApplyNum(applyNum);
            		payment.setStatus(status);
            		payment.setPgProvider(pgProvider);
            		payment.setPgMethod(pgMethod);
            		payment.setBuyerName(buyerName);
            		payment.setPaidAt(dateTimeFormat.format(new Date((long)paidAt * 1000)));
            		payment.setAmount(amount);
            		payment.setTotalCancelledAmount(totalCancelledAmount);
            		payment.setCancelledAmount(historyObj.getInt("amount"));
            		
            		cancelledAt = historyObj.getInt("cancelled_at");
            		payment.setCancelledAt(dateTimeFormat.format(new Date((long)cancelledAt * 1000)));
            		payment.setCancelReson(historyObj.getString("reason"));
            		
            		paymentList.add(payment);
        		}
    		}
    	}
    	
    	resultMap.put("total", response.get("total"));
    	resultMap.put("next", response.get("next"));
    	resultMap.put("list", paymentList);
    	
    	return resultMap;
    }
}
