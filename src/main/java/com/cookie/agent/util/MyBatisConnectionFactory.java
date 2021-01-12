package com.cookie.agent.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.log4j.Logger;

/**
 * SqlSessionFactory 생성해서 DAO 클래스에서 사용하도록 제공
 */
public class MyBatisConnectionFactory {
    
    private static final Logger logger = Logger.getLogger(MyBatisConnectionFactory.class);
    private static SqlSessionFactory sqlSessionFactory;

    static {
        try {
            String resource = "sqls/mybatis-config.xml";
            Reader reader = Resources.getResourceAsReader(MyBatisConnectionFactory.class.getClassLoader(), resource);
            
            // Build a SqlSessionFactory instance from an XML file
            if (sqlSessionFactory == null) {
                sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);
            }
        } catch(FileNotFoundException fileNotFoundException) {
            logger.error(ExceptionUtils.getStackTrace(fileNotFoundException));
        } catch (IOException ioException) {
            logger.error(ExceptionUtils.getStackTrace(ioException));
        }
    }

    /**
     * @return SqlSessionFactory
     */
    public static SqlSessionFactory getSqlSessionFactory() {
        return sqlSessionFactory;
    }
}
