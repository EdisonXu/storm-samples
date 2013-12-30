/**
 * 
 */
package com.edi.storm.util;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author Edison Xu
 * 
 *         Dec 4, 2013
 */
public class DBManager {

	private static final DBManager instance = new DBManager();

	private static ComboPooledDataSource cpds = new ComboPooledDataSource(true);

	static {
		cpds.setDataSourceName("mydatasource");
		cpds.setJdbcUrl("jdbc:mysql://10.1.110.21:3306/metadata?useUnicode=true&amp;characterEncoding=utf8");
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		cpds.setUser("root");
		cpds.setPassword("111111");
		cpds.setMaxPoolSize(10);
		cpds.setMinPoolSize(2);
		cpds.setAcquireIncrement(5);
		cpds.setInitialPoolSize(5);
		cpds.setMaxIdleTime(9);
	}
	
	private DBManager(){}  
	public static DBManager getInstance(){  
        return instance;  
    }  
      
    public static Connection  getConnection(){  
        try {  
            return cpds.getConnection();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
        return null;  
    }  
}
