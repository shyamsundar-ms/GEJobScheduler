package com.ge.job.scheduler.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by User on 13/12/2016.
 */
@Component
public class GEDatabaseConnection {

    private Connection connection;

    @Autowired
    public GEDatabaseConnection(DataSource dataSource) {
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getDatabaseConnection(){
        return connection;
    }

    /*private static Connection getDBConnection() {

        System.out.println("Connecting Mysql server");
        java.sql.Connection con = null;
        try {

            Properties properties = null;
            try {
                properties = loadPropertiesFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String driverClass = properties.getProperty("MYSQLJDBC.driver");
            String url = properties.getProperty("MYSQLJDBC.url");
            String username = properties.getProperty("MYSQLJDBC.username");
            String password = properties.getProperty("MYSQLJDBC.password");
            Class.forName(driverClass);
            con = DriverManager.getConnection(url,username,password);
            if (con !=null){
                System.out.println("Connected Successfully");
            }
            else{
                System.out.println("unable to connected the mysql server");
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }

    private static Properties loadPropertiesFile() throws IOException {

        Properties properties = new Properties();
        InputStream in = new FileInputStream("classpath:jdbc.properties");
        properties.load(in);
        in.close();
        return properties;
    }*/

}
