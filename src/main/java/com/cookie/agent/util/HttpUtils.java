package com.cookie.agent.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

public class HttpUtils {

    private static final Logger logger = Logger.getLogger(HttpUtils.class);
    private static final int maxRequestCnt = 2;
    private static final int timeout = 180000; //milliseconds.

    /**
     * HTTP Request
     *
     * @param requestUrl	request URL
     * @param method		HTTP method
     * @param headers 		header information map
     * @param body			request body
     */
    public static String request(String url, String method, Map<String, Object> headers, String body) {
        int requestCnt = 0;

        Thread thread = null;

        while (requestCnt < maxRequestCnt) {
            HttpURLConnection connection = null;
            BufferedReader reader = null;

            try {
                connection = createHttpURLConnection(new URL(url), method, headers);

                thread = new Thread(new InterruptThread(connection));
                thread.start();

                if (body != null) {
                    DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                    wr.write(body.getBytes("UTF-8"));
                    wr.flush();
                    wr.close();
                }

                // Get Response
                InputStream is;
                try {
                    is = connection.getInputStream();
                } catch (IOException e) {
                    is = connection.getErrorStream();
                    logger.error(ExceptionUtils.getStackTrace(e));
                }

                reader = new BufferedReader(new InputStreamReader(is));
                String line;
                StringBuilder response = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                    //response.append('\r');
                    response.append('\n');
                }

                int responseCode = connection.getResponseCode();
                if (responseCode == 200) {          //200:OK
                	return response.toString();
                } else if (responseCode == 202) {   //202:ProcessingNotCompleted (Azure API Error Code)
                    return null;
                }

                //throw new Exception(String.format("response code = %d, error message = %s", responseCode, response.toString()));
                logger.error(String.format("response code = %d, error message = %s", responseCode, response.toString()));
            } catch (Exception e) {
                logger.error(ExceptionUtils.getStackTrace(e));
            } finally {
                if (thread != null) {
                    //try {
                    //    thread.join();
                    //} catch (InterruptedException e) {
                    //    e.printStackTrace();
                    //}

                    if (thread.isAlive()) {
                        thread.interrupt();
                    }
                }

                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        logger.error(ExceptionUtils.getStackTrace(e));
                    }
                }

                if (connection != null) {
                    connection.disconnect();
                }
            }

            requestCnt++;
            logger.info("retry count = " + requestCnt);
        }

        return null;
    }

    /**
     * Create HttpURLConnection
     *
     * @param requestUrl	request URL
     * @param method		HTTP method
     * @param headers 		header information map
     */
    private static HttpURLConnection createHttpURLConnection(URL requestUrl, String method, Map<String, Object> headers) {
        try {
            boolean isSSL = requestUrl.getProtocol().startsWith("https");
            if (isSSL == true) {
                SSLContext sslContext = SSLContext.getInstance("SSL");

                TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() { return null; }
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                    }
                };

                sslContext.init(null, trustAllCerts, new SecureRandom());
                HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            }

            HttpURLConnection connection = (HttpURLConnection)requestUrl.openConnection();
            connection.setRequestMethod(method);

            if (headers != null) {
                //System.out.println("--------- Request headers ---------");
                for (String headerKey : headers.keySet()) {
                    //System.out.println(headerKey + ": " + headers.get(headerKey));
                    connection.setRequestProperty(headerKey, headers.get(headerKey).toString());
                }
            }

            connection.setConnectTimeout(timeout);
            connection.setReadTimeout(timeout);
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            return connection;
        } catch (Exception e) {
            throw new RuntimeException("Cannot create connection. " + e.getMessage(), e);
        }
    }

    /**
     * Disconnect connection
     */
    private static class InterruptThread implements Runnable {

        HttpURLConnection con;
        public InterruptThread(HttpURLConnection con) {
            this.con = con;
        }

        public void run() {
            try {
                Thread.sleep(timeout);

                con.disconnect();
                logger.info("Timer thread timeout");
            } catch (InterruptedException e) {

            }
        }
    }
}
