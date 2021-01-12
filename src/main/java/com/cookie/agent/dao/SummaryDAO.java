package com.cookie.agent.dao;

import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import com.cookie.agent.CookieBatchManager.SummaryType;
import com.cookie.agent.model.DatePeriod;

public class SummaryDAO {

    private SqlSessionFactory sqlSessionFactory = null;

    public SummaryDAO(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }
    
    /**
     * Get max history no from TBS_BIKE_USE_HISTORY table
     *
     * @return int	max history no
     */
    public int selectBikeUseMaxHistoryNo() {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectBikeUseMaxHistoryNo");
        } finally {
            session.close();
        }

        return (result == null) ? 0 : (Integer)result;
    }
    
    /**
     * Get max history no from TBS_CASH_POINT_HISTORY table
     *
     * @return int	max history no
     */
    public int selectCashPointMaxHistoryNo() {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectCashPointMaxHistoryNo");
        } finally {
            session.close();
        }

        return (result == null) ? 0 : (Integer)result;
    }
    
    /**
     * Get max history no from TBS_VOC_PROC_HISTORY table
     *
     * @return int	max history no
     */
    public int selectVocProcessMaxHistoryNo() {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectVocProcessMaxHistoryNo");
        } finally {
            session.close();
        }

        return (result == null) ? 0 : (Integer)result;
    }
    
    /**
     * Get min bike use start date in specific period from TBS_BIKE_USE_HISTORY table
     *
     * @param map		update start date
     * @return String	min bike use start date ('yyyy-MM-dd')
     */
    public String selectBikeHistoryStartDate(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectBikeHistoryStartDate", map);
        } finally {
            session.close();
        }

        return (result == null) ? null : (String)result;
    }
    
    /**
     * Get min start date in specific period from TBS_STATS_CUSTOMER_DAY_SUMMARY table
     *
     * @param map		update start date
     * @return String	min start date ('yyyy-MM-dd')
     */
    public String selectCustomerDailySummaryStartDate(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectCustomerDailySummaryStartDate", map);
        } finally {
            session.close();
        }

        return (result == null) ? null : (String)result;
    }
    
    /**
     * Get min start date in specific period from TBS_STATS_BIKE_DAY_SUMMARY table
     *
     * @param map		update start date
     * @return String	min start date ('yyyy-MM-dd')
     */
    public String selectBikeDailySummaryStartDate(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectBikeDailySummaryStartDate", map);
        } finally {
            session.close();
        }

        return (result == null) ? null : (String)result;
    }
    
    /**
     * Get min start date in specific period from TBS_STATS_PAYMENT_DAY_SUMMARY table
     *
     * @param map		update start date
     * @return String	min start date ('yyyy-MM-dd')
     */
    public String selectPaymentDailySummaryStartDate(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectPaymentDailySummaryStartDate", map);
        } finally {
            session.close();
        }

        return (result == null) ? null : (String)result;
    }
    
    /**
     * Get min start date in specific period from TBS_STATS_VOC_DAY_SUMMARY table
     *
     * @param map		update start date
     * @return String	min start date ('yyyy-MM-dd')
     */
    public String selectVocDailySummaryStartDate(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        Object result;

        try {
        	result = session.selectOne("Summary.selectVocDailySummaryStartDate", map);
        } finally {
            session.close();
        }

        return (result == null) ? null : (String)result;
    }
    
    /**
     * Update device status statistics in TBS_STATS_BIKE_TYPE, TBS_STATS_DEVICE_STATUS table
     */
    public void updateCurrentDeviceStatus() {
        SqlSession session = sqlSessionFactory.openSession();

        try {
        	session.update("Summary.resetStatsBikeTypeCount");
        	session.update("Summary.resetStatsDeviceStatus");
        	
            session.update("Summary.updateBikeTypeDistributionStatus");
            session.update("Summary.updateServiceStatus");
            session.update("Summary.updateCommunicationStatus");
            session.update("Summary.updateBikeStatus");
            session.commit();
        } finally {
            session.close();
        }
    }
    
    /**
     * Update accumulation statistics data in TBS_STATS_BIKE_TYPE, TBS_BIKE, TBS_CUSTOMER_DETAIL table
     * 
     * @param map 	history number range
     */
    public void accumulationData(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.update("Summary.updateUseCountByBikeType", map);
            session.update("Summary.updateBikeAccumulationData1", map);
            session.update("Summary.updateCustomerBikeAccumulationData1", map);
            session.update("Summary.updateCustomerPaymentAccumulationData", map);
            
            if (map != null && map.get("updateStartDate") != null && map.get("updateEndDate") != null) {
            	session.update("Summary.updateBikeAccumulationData2", map);
            	session.update("Summary.updateCustomerBikeAccumulationData2", map);
            }
            
            session.commit();
        } finally {
            session.close();
        }
    }
    
    /**
     * Update customer detail in TBS_CUSTOMER_DETAIL table
     * 
     * @param map 	history number range
     */
    public void updateCustomerDetailData(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.update("Summary.updateCustomerDetailData", map);
            session.commit();
        } finally {
            session.close();
        }
    }
    
    /**
     * Get period to summary payment statistics from TBS_VOC_PROC_HISTORY, TBS_MGMT_VOC_PAYMENT table
     *
     * @param map 			history number range
     * @return DatePeriod	payment period
     */
    public DatePeriod selectUpdatePaymentPeriod(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        DatePeriod result = null;

        try {
        	result = session.selectOne("Summary.selectUpdatePaymentPeriod", map);
        } finally {
            session.close();
        }

        return result;
    }
    
    /**
     * Insert/Update customer statistics summary into TBS_STATS_CUSTOMER_DAY_SUMMARY, TBS_STATS_CUSTOMER_WEEK_SUMMARY, TBS_STATS_CUSTOMER_MONTH_SUMMARY table
     *
     * @param map 						summary start/end date
     * @param type						summary type
     * @param isAvailableUpdateCustomer	update true or false
     */
    public void summaryCustomerData(Map<String, Object> map, SummaryType type, boolean isAvailableUpdateCustomer) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            switch (type) {
            case DAILY:
            	if (isAvailableUpdateCustomer) {
            		session.update("Summary.summaryDailyRegistCustomer", map);
                    session.update("Summary.summaryDailyUnregistCustomer", map);
            	}
                break;
            case WEEKLY:
            	session.update("Summary.summaryWeeklyCustomer", map);
                break;
            case MONTHLY:
            	session.update("Summary.summaryMonthlyCustomer", map);
                break;
            default:
                break;
            }

            session.commit();
        } finally {
            session.close();
        }
    }
    
    /**
     * Insert/Update summary data about bike history statistics into TBS_STATS_BIKE_LOC_DAY_SUMMARY, TBS_STATS_BIKE_DAY_SUMMARY, TBS_STATS_BIKE_WEEK_SUMMARY, TBS_STATS_BIKE_MONTH_SUMMARY table
     *
     * @param map 						summary start/end date
     * @param type						summary type
     */
    public void summaryBikeHistoryData(Map<String, Object> map, SummaryType type) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            switch (type) {
            case DAILY:            	
                session.update("Summary.summaryDailyBikeHistoryByLoc", map);
                session.update("Summary.summaryDailyBikeHistory", map);
                break;
            case WEEKLY:
                session.update("Summary.summaryWeeklyBikeHistory", map);
                break;
            case MONTHLY:
                session.update("Summary.summaryMonthlyBikeHistory", map);
                break;
            default:
                break;
            }

            session.commit();
        } finally {
            session.close();
        }
    }
    
    /**
     * Insert/Update summary data about payment history statistics into TBS_STATS_PAYMENT_DAY_SUMMARY, TBS_STATS_PAYMENT_WEEK_SUMMARY, TBS_STATS_PAYMENT_MONTH_SUMMARY
     *
     * @param map 						summary start/end date
     * @param type						summary type 
     * @param isAvailableUpdateCustomer	update true or false
     */
    public void summaryPaymentHistoryData(Map<String, Object> map, SummaryType type, boolean isAvailableUpdateCustomer) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            switch (type) {
            case DAILY:
            	if (isAvailableUpdateCustomer) {
            		session.update("Summary.summaryDailyPaymentHistory", map);
                    //session.update("Summary.updateSummaryDailyPaymentHistory", map);
            	}
                break;
            case WEEKLY:
                session.update("Summary.summaryWeeklyPaymentHistory", map);
                break;
            case MONTHLY:
                session.update("Summary.summaryMonthlyPaymentHistory", map);
                break;
            default:
                break;
            }

            session.commit();
        } finally {
            session.close();
        }
    }
    
    /**
     * Insert/Update summary data about voc history statistics into TBS_STATS_VOC_DAY_SUMMARY, TBS_STATS_VOC_WEEK_SUMMARY, TBS_STATS_VOC_MONTH_SUMMARY
     *
     * @param map 						summary start/end date
     * @param type						summary type
     */
    public void summaryVocHistoryData(Map<String, Object> map, SummaryType type) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            switch (type) {
            case DAILY:
                session.update("Summary.summaryDailyVocHistory", map);
                break;
            case WEEKLY:
                session.update("Summary.summaryWeeklyVocHistory", map);
                break;
            case MONTHLY:
                session.update("Summary.summaryMonthlyVocHistory", map);
                break;
            default:
                break;
            }

            session.commit();
        } finally {
            session.close();
        }
    }
}
