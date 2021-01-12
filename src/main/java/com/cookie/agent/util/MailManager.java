package com.cookie.agent.util;

import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.log4j.Logger;

import com.cookie.agent.dao.CommonDAO;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * SMTP를 사용하여 이메일 전송을 위한 함수 제공
 */
public class MailManager {
    private static final Logger logger = Logger.getLogger(MailManager.class);
    private static MailManager instance = null;
    
    private String smtpHost;
    private String smtpUser;
    private String smtpPassword;
    private String mailFrom;
    private String mailTo;
    
    static {
        try {
            if (instance == null) {
            	SqlSessionFactory sqlSessionFactory = MyBatisConnectionFactory.getSqlSessionFactory();
    	        CommonDAO commonDao = new CommonDAO(sqlSessionFactory);
    	        
    	        HashMap<String, String> smtpInfoMap = new HashMap<String, String>();
    	        List<HashMap> mapList = commonDao.selectSmtpInfo();
    	        for (HashMap map : mapList) {
    	            smtpInfoMap.put(map.get("keyName").toString(), map.get("value").toString());
    	        }
            	
                instance = new MailManager(smtpInfoMap.get("SMTP_HOST").toString(), smtpInfoMap.get("SMTP_USER").toString(), smtpInfoMap.get("SMTP_PASSWORD").toString(), smtpInfoMap.get("MAIL_FROM").toString());
            }
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * MailManager Constructor
     * 
     * @param smtpHost		the host to connect
     * @param smtpUser		user name
     * @param smtpPassword	user password
     * @param mailFrom		the sender of this message
     */
    private MailManager(String smtpHost, String smtpUser, String smtpPassword, String mailFrom) {
        this.smtpHost = smtpHost;
        this.smtpUser = smtpUser;
        this.smtpPassword = smtpPassword;
        this.mailFrom = mailFrom;
        this.mailTo = ConfigurationManager.getConfiguration().getString("admin_email");
    }
    
    /**
     * Get MailManager Instance
     */
    public static MailManager getInstance() {
        return instance;
    }

    /**
     * Send email
     * 
     * @param subject	mail subject
     * @param body		mail content
     */
    public void sendEmail(String subject, String body) {
        try {
            String[] toList = mailTo.toString().split(";");

            // Port we will connect to on the Amazon SES SMTP endpoint. We are choosing port 25 because we will use
            // STARTTLS to encrypt the connection.
            // Create a Properties object to contain connection configuration information.
            Properties props = System.getProperties();
            props.put("mail.transport.protocol", "smtp");
            props.put("mail.smtp.port", 587);

            // Set properties indicating that we want to use STARTTLS to encrypt the connection.
            // The SMTP session will begin on an unencrypted connection, and then the client
            // will issue a STARTTLS command to upgrade to an encrypted connection.
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.starttls.required", "true");

            // Create a Session object to represent a mail session with the specified properties.
            Session session = Session.getDefaultInstance(props);

            // Create a message with the specified information.
            MimeMessage msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(mailFrom));

            InternetAddress[] address = new InternetAddress[toList.length];
            for (int i = 0; i < toList.length; i++) {
                address[i] = new InternetAddress(toList[i]);
            }
            msg.setRecipients(javax.mail.Message.RecipientType.TO, address);

            msg.setSubject(subject, "UTF-8");
            msg.setContent(body, "text/html; charset=UTF-8");

            // Create a transport.
            Transport transport = session.getTransport();

            logger.info("Attempting to send an email through the Amazon SES SMTP interface...");

            // Connect to Amazon SES using the SMTP username and password you specified above.
            transport.connect(smtpHost, smtpUser, smtpPassword);

            // Send the email.
            transport.sendMessage(msg, msg.getAllRecipients());
            logger.info("Email sent!");

            transport.close();
        } catch(Exception e) {
            logger.error("The email was not sent.");
            logger.error("Error message: " + e.getMessage());
        }
    }
}
