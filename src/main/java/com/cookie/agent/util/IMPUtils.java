package com.cookie.agent.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class IMPUtils {

    private static final Logger logger = Logger.getLogger(IMPUtils.class);

    /**
     * Get I'mport token
     *
     * @param impKey		iamport rest api key
     * @param impSecret		iamport rest api secret
     */
    public static String getToken(String impKey, String impSecret) {
    	String url = String.format("https://api.iamport.kr/users/getToken");
    	
    	Map<String, Object> headers = new HashMap<String, Object>();
    	headers.put("Content-Type", "application/json;charset=UTF-8");
    	
    	JSONObject obj = new JSONObject();
    	obj.put("imp_key", impKey);
    	obj.put("imp_secret", impSecret);

        String responseData = HttpUtils.request(url, "POST", headers, obj.toString());
        JSONObject result = JSONObject.fromObject(JSONSerializer.toJSON(responseData));
        
        return result.getJSONObject("response").getString("access_token");
    }

    /**
     * Get Payment History List
     *
     * @param token		iamport rest api token
     * @param status	payment status
     * @param page		page no (start no is 1)
     * @param limit		payment count to retrieve
     * @param from		start time (unix timestamp)
     * @param to		end time (unix timestamp)
     */
    public static String getPaymentHistoryList(String token, String status, int page, int limit, int from, int to) {
        String url = String.format("https://api.iamport.kr/payments/status/%s?page=%d&limit=%d&to=%d&sorting=started&_token=%s", status, page, limit, to, token);
        if (from > 1) {
        	url += ("&from=" + from);
        }
        
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("Content-Type", "application/json;charset=UTF-8");
        
        String responseData = HttpUtils.request(url, "GET", headers, null);
        return responseData;
    }
}
