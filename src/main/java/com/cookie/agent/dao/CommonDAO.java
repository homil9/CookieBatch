package com.cookie.agent.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import com.cookie.agent.model.AgentHistory;
import com.cookie.agent.model.PaymentInfo;

public class CommonDAO {

    private SqlSessionFactory sqlSessionFactory = null;

    public CommonDAO(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    /**
     * Insert/Update cash/point history about expired point in TBS_CASH_POINT_HISTORY table
     */
    public void updateExpiredPoint() {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.update("Common.updateExpiredPoint");

            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Get expire point information by customer from TBS_CUSTOMER, TBS_CASH_POINT_HISTORY table
     *
     * @return List<Map < String, Object>>	customer information list
     */
    public List<Map<String, Object>> selectExpiringPoint() {
        SqlSession session = sqlSessionFactory.openSession();
        List<Map<String, Object>> list = null;

        try {
            list = session.selectList("Common.selectExpiringPoint");
        } finally {
            session.close();
        }
        session.close();
        return list;
    }

    /**
     * Update expire point in TBS_CUSTOMER
     *
     * @param map customer no, expire point information
     */
    public void updateExpiringPoint(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.update("Common.updateExpiringPoint", map);
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Update dormant customer status in TBS_CUSTOMER
     */
    public void updateDormantCustomer() {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.update("Common.updateAwakeCustomer");
            session.update("Common.updateDormantCustomer");
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Delete customer information from TBS_CUSTOMER, TBS_CUSTOMER_DETAIL
     */
    public void deleteCustomer() {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.update("Common.updateToBeDeleteCustomer");
            session.delete("Common.deleteCustomerDetail");
            session.delete("Common.deleteCustomer");
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Delete cash/point history from TBS_CASH_POINT_HISTORY
     */
    public void deleteCashPointHistory() {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.delete("Common.deleteCashPointHistory");
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Delete bike use history from TBS_BIKE_USE_HISTORY
     */
    public void deleteBikeUseHistory() {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.delete("Common.deleteBikeUseHistory");
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Delete voc data from TBS_MGMT_VOC, TBS_MGMT_VOC_DEVICE, TBS_MGMT_VOC_PAYMENT, TBS_VOC_PROC_HISTORY
     */
    public void deleteVocData() {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.delete("Common.deleteVocProcessHistory");
            session.delete("Common.deleteVocData");
            session.delete("Common.deleteDeviceVocData");
            session.delete("Common.deletePaymentVocData");
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    public List<AgentHistory> selectAgentHistory(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        List<AgentHistory> list = null;

        try {
            list = session.selectList("Common.selectAgentHistory", map);
        } finally {
            session.close();
        }
        session.close();
        return list;
    }

    /**
     * Insert agent execution history into TBS_AGENT_HISTORY table
     *
     * @param map history information
     */
    public void insertAgentHistory(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.insert("Common.insertAgentHistory", map);
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Get SMTP information (host, user, password, from) from TBS_DEVELOP_CONFIG table
     *
     * @return List<HashMap>	SMTP information
     */
    public List<HashMap> selectSmtpInfo() {
        SqlSession session = sqlSessionFactory.openSession();
        List<HashMap> result = null;

        try {
            result = session.selectList("Common.selectSmtpInfo");
        } finally {
            session.close();
        }
        session.close();
        return result;
    }

    /**
     * Get I'amport information (api key, api secret key) from TBS_DEVELOP_CONFIG table
     *
     * @return List<HashMap>	I'amport information
     */
    public List<HashMap> selectIamportInfo() {
        SqlSession session = sqlSessionFactory.openSession();
        List<HashMap> result = null;

        try {
            result = session.selectList("Common.selectIamportInfo");
        } finally {
            session.close();
        }
        session.close();
        return result;
    }

    /**
     * Get I'amport payment history to synchronize from TBS_PAYMENT_IMP table
     *
     * @return List<HashMap>	I'amport payment history list
     */
    public List<HashMap> selectIMPPaymentHistory(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        List<HashMap> result = null;

        try {
            result = session.selectList("Common.selectIMPPaymentHistory", map);
        } finally {
            session.close();
        }
        session.close();
        return result;
    }

    /**
     * Insert I'amport payment history into TBS_PAYMENT_IMP table
     *
     * @param map history information
     */
    public void insertIMPPaymentHistory(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.insert("Common.insertIMPPaymentHistory", map);
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Get payment information from TBS_PAYMENT table
     *
     * @param map payment no
     * @return PaymentInfo    payment information
     */
    public PaymentInfo selectPaymentInfo(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        PaymentInfo result = null;

        try {
            result = session.selectOne("Common.selectPayment", map);
        } finally {
            session.close();
        }
        session.close();
        return result;
    }

    /**
     * Update payment information in TBS_PAYMENT, TBS_CUSTOMER, TBS_CASH_POINT_HISTORY table
     *
     * @param map payment information
     */
    public void updatePaymentInfo(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();

        try {
            session.update("Common.updatePayment", map);
            session.insert("Common.insertCashPointHistory", map);
            session.update("Common.updateCustomerInfo", map);
            session.commit();
        } finally {
            session.close();
        }
        session.close();
    }

    /**
     * Get I'amport payment history to synchronize from TBS_PAYMENT_IMP table
     * iamport paid != cookie wait select list
     *
     * @return List<HashMap>	I'amport payment history list
     */
    public List<HashMap> selectIMPPaymentHistory2(Map<String, Object> map) {
        SqlSession session = sqlSessionFactory.openSession();
        List<HashMap> result = null;

        try {
            result = session.selectList("Common.selectIMPPaymentHistory2", map);
        } finally {
            session.close();
        }
        session.close();
        return result;

    }
}
