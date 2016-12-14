package com.ge.job.scheduler.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ge.job.scheduler.bo.ProductItemsBO;
import com.ge.job.scheduler.bo.ProductMetaDataBO;
import com.ge.job.scheduler.util.GEDatabaseConnection;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Created by User on 13/12/2016.
 */
@Service
public class GECrunchBaseCSVDownloader {

    @Value("${crunchbase.userkey}")
    private String crunchbaseUserKey;

    @Value("${csv.download.path}")
    private String csvDownloadPath;

    @Autowired
    private GEDatabaseConnection dbConnection;

    public void downloadCsvAndLoadInputTables(){
        downloadAndExtractCsvFiles();
        loadCsvTablesFromCsvFiles();
        getProducts();
        callAllJobsSP();
    }

    //1
    private void downloadAndExtractCsvFiles() {
        String BASE_URL="https://api.crunchbase.com/v/3/csv_export/csv_export.tar.gz?user_key=" + crunchbaseUserKey;

        try {
            Long start = System.nanoTime();
            System.out.println("\n\nDownloading Crunchbase CSV zip file Started!--------->"+ Calendar.getInstance().getTime());
            URL website = new URL(BASE_URL);
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream("csv_export.tar.gz");
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            Long end = System.nanoTime();
            long duration = end - start;
            duration = TimeUnit.MINUTES.convert(duration, TimeUnit.NANOSECONDS);
            System.out.println("Downloading zip Completed!. Total time taken for downloading CSV file ---> " + duration + " mins\n");
            fos.close();

            start = System.nanoTime();
            System.out.println("Decompressing CSV gzip! File Started---------"+ Calendar.getInstance().getTime());
            File csvDownloadDirectory = new File(csvDownloadPath);
            if(csvDownloadDirectory.isDirectory()){
                FileUtils.cleanDirectory(csvDownloadDirectory);
            }
            Archiver archiver = ArchiverFactory.createArchiver("tar", "gz");
            archiver.extract(new File("csv_export.tar.gz"), csvDownloadDirectory);

            File gzipFile = new File("csv_export.tar.gz");
            if (gzipFile.exists()){
                gzipFile.delete();
            }
            end = System.nanoTime();
            duration = end - start;
            duration = TimeUnit.MINUTES.convert(duration, TimeUnit.NANOSECONDS);
            System.out.println("Decompressing zip File Completed. Total time taken for decompressing zip file --> " + duration + " mins\n");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    //2
    private void loadCsvTablesFromCsvFiles() {
        Statement stmt = null;
        Connection con = dbConnection.getDatabaseConnection();
        try {
            String loadQuery = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "organizations.csv" + "' INTO TABLE cb_csv_tbl_organizations CHARACTER SET utf8mb4  FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (company_name, primary_role, domain,homepage_url,country_code,state_code,region,city,zipcode,address,status,short_description,category_list,category_group_list,funding_rounds," +
                    "funding_total_usd,founded_on,first_funding_on,last_funding_on,closed_on,employee_count,email,phone,facebook_url,linkedin_url,cb_url,logo_url,profile_image_url,twitter_url,uuid,created_at,updated_at) ";

            String loadQuery1 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "organization_descriptions.csv" + "' INTO TABLE cb_csv_tbl_organization_descriptions  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (uuid, description) ";

            String loadQuery2 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "people.csv" + "' INTO TABLE cb_csv_tbl_people CHARACTER SET utf8mb4  FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' " + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (first_name, last_name,country_code,state_code,city,cb_url,logo_url,profile_image_url,twitter_url,facebook_url,primary_affiliation_organization,primary_affiliation_title,primary_organization_uuid,gender,uuid,created_at,updated_at) ";

            String loadQuery3 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "funding_rounds.csv" + "' INTO TABLE cb_csv_tbl_funding_rounds CHARACTER SET utf8mb4  FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"' " + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (company_name, country_code,state_code,region,city,company_category_list,funding_round_type,funding_round_code,announced_on,raised_amount_usd,raised_amount,raised_amount_currency_code,target_money_raised_usd,target_money_raised,target_money_raised_currency_code,post_money_valuation_usd,post_money_valuation," +
                    "post_money_currency_code,investor_count,investor_names,cb_url,company_uuid,funding_round_uuid,created_at,updated_at) ";

            String loadQuery4 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "funds.csv" + "' INTO TABLE cb_csv_tbl_funds  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (entity_uuid, fund_uuid,fund_name,started_on,announced_on,raised_amount,raised_amount_currency_code,created_at,updated_at) ";

            String loadQuery5 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "acquisitions.csv" + "' INTO TABLE cb_csv_tbl_acquisitions  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (acquiree_name, acquiree_country_code,state_code,acquiree_region,acquiree_city,acquirer_name,acquirer_country_code,acquirer_state_code,acquirer_region,acquirer_city,acquisition_type,acquired_on,price_usd,price,price_currency_code,acquiree_cb_url,acquirer_cb_url,acquiree_uuid,acquirer_uuid,acquisition_uuid,created_at,updated_at) ";


            String loadQuery6 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "awards.csv" + "' INTO TABLE cb_csv_tbl_awards  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (type, award_name,award_uuid,event_name,event_uuid,awardee_name,awardee_uuid) ";

            String loadQuery7 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "competitors.csv" + "' INTO TABLE cb_csv_tbl_competitors  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (entity_uuid, competitor_uuid,created_at,updated_at) ";

            String loadQuery8 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "customers.csv" + "' INTO TABLE cb_csv_tbl_customers  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (entity_uuid, customer_uuid,created_at,updated_at) ";

            String loadQuery9 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "investments.csv" + "' INTO TABLE cb_csv_tbl_investments  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (funding_round_uuid, investor_uuid,is_lead_investor) ";

            String loadQuery10 = "LOAD DATA LOCAL INFILE '" + csvDownloadPath + File.pathSeparator + "investors.csv" + "' INTO TABLE cb_csv_tbl_investors  CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'" + " LINES TERMINATED BY '\n' " +
                    "IGNORE 1 LINES (investor_name, primary_role,domain,country_code,state_code,region,city,investor_type,investment_count,total_funding_usd,founded_on,closed_on,cb_url,logo_url,profile_image_url,twitter_url,facebook_url,uuid,updated_at) ";


            Long start = System.nanoTime();
            System.out.println("CSV Database Import Started!---------"+ Calendar.getInstance().getTime());
            con.setAutoCommit(false);

            stmt = con.createStatement();

            stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            stmt.execute("SET unique_checks = 0;");
            stmt.execute("SET sql_log_bin=0");
            stmt.execute("SET NAMES 'utf8mb4' ");
            stmt.execute("TRUNCATE TABLE cb_csv_tbl_organizations");
            System.out.println(loadQuery);
            stmt.execute(loadQuery);
            System.out.println("Loading cb_csv_tbl_organizations completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_organization_descriptions");
            System.out.println(loadQuery1);
            stmt.execute(loadQuery1);
            System.out.println("Loading cb_csv_tbl_organization_descriptions completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_people");
            System.out.println(loadQuery2);
            stmt.execute(loadQuery2);
            System.out.println("Loading cb_csv_tbl_people completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_funding_rounds");
            System.out.println(loadQuery3);
            stmt.execute(loadQuery3);
            System.out.println("Loading cb_csv_tbl_funding_rounds completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_funds");
            System.out.println(loadQuery4);
            stmt.execute(loadQuery4);
            System.out.println("Loading cb_csv_tbl_funds completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_acquisitions");
            System.out.println(loadQuery5);
            stmt.execute(loadQuery5);
            System.out.println("Loading cb_csv_tbl_acquisitions completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_awards");
            System.out.println(loadQuery6);
            stmt.execute(loadQuery6);
            System.out.println("Loading cb_csv_tbl_awards completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_competitors");
            System.out.println(loadQuery7);
            stmt.execute(loadQuery7);
            System.out.println("Loading cb_csv_tbl_competitors completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_customers");
            System.out.println(loadQuery8);
            stmt.execute(loadQuery8);
            System.out.println("Loading cb_csv_tbl_customers completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_investments");
            System.out.println(loadQuery9);
            stmt.execute(loadQuery9);
            System.out.println("Loading cb_csv_tbl_investments completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_csv_tbl_investors");
            System.out.println(loadQuery10);
            stmt.execute(loadQuery10);
            System.out.println("Loading cb_csv_tbl_investors completed ---------> "+ Calendar.getInstance().getTime() + "\n");

            stmt.execute("TRUNCATE TABLE cb_rest_api_tbl_product");
            con.commit();

            stmt.execute("SET FOREIGN_KEY_CHECKS=1");
            stmt.execute("SET unique_checks = 1;");
            stmt.execute("SET sql_log_bin=1");
            stmt.execute("SET NAMES 'utf8' ");

            Long end = System.nanoTime();
            long duration = end - start;

            duration = TimeUnit.MINUTES.convert(duration, TimeUnit.NANOSECONDS);
            System.out.println("CSV Database Import Completed. Total time taken for Importing CSV data to database --> " + duration + " mins\n");

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                con.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            /*if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    //3
    private void getProducts() {
        int num_of_page = 1;
        String productApiUrl = null;
        Long start = System.nanoTime();
        System.out.println("Getting Products Started!---------"+ Calendar.getInstance().getTime());

        try {
            for (int pageNumber = 1; pageNumber <= num_of_page; pageNumber++) {
                //https://api.crunchbase.com/v/3/products?page=1&user_key=xxxx
                productApiUrl = "https://api.crunchbase.com/v/3/products?page=" + pageNumber + "&" + "user_key=" + crunchbaseUserKey;
                HttpClient client = HttpClientBuilder.create().build();
                HttpGet request = new HttpGet(productApiUrl);
                HttpResponse response = null;
                response = client.execute(request);
                System.out.println("Response Code : "
                        + response.getStatusLine().getStatusCode());
                BufferedReader rd = new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent()));
                StringBuffer resultJsonString = new StringBuffer();
                String line = "";
                while ((line = rd.readLine()) != null) {
                    resultJsonString.append(line);
                }
                ObjectMapper mapper = new ObjectMapper();
                ProductMetaDataBO productMetaDataBO = mapper.readValue(resultJsonString.toString(), ProductMetaDataBO.class);
                List<ProductItemsBO> productItemsBOs = productMetaDataBO.getData().getItems();
                insertIntoProductTable(productItemsBOs);
                num_of_page = productMetaDataBO.getData().getPaging().getNumberOfPages();
                System.out.println(pageNumber + "***********************************************************************");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Long end = System.nanoTime();
        long duration = end - start;

        duration = TimeUnit.MINUTES.convert(duration, TimeUnit.NANOSECONDS);
        System.out.println("Getting Products Completed. Total time taken to get the Products & load it to database --> " + duration + " mins\n");
    }

    //3.1
    private void insertIntoProductTable(List<ProductItemsBO> productItemsBOs) throws SQLException {

        Connection con = null;
        PreparedStatement preparedStatementInsert = null;
        String insertQuery ="INSERT INTO `cb_rest_api_tbl_product` (`uuid`, `type`, `permalink`, `api_path`,`web_path`, `name`,`short_description`,`owner_permalink`,`owner_web_path`,`owner_name`, `profile_image_url`, `homepage_url`,`facebook_url`, `twitter_url`,`linkedin_url`,`created_at`,`updated_at`,`blog_url`,`google+_url`,`instagram_url`,`pinterest_url`)\n" +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try {
            con = dbConnection.getDatabaseConnection();
            con.setAutoCommit(false);

            preparedStatementInsert = con.prepareStatement(insertQuery);

            for(ProductItemsBO productItemsBO : productItemsBOs) {
                preparedStatementInsert.setString(1, productItemsBO.getUuid());
                preparedStatementInsert.setString(2, productItemsBO.getType());
                preparedStatementInsert.setString(3, productItemsBO.getProperties().getPermalink());
                preparedStatementInsert.setString(4, productItemsBO.getProperties().getApiPath());
                preparedStatementInsert.setString(5, productItemsBO.getProperties().getWebPath());
                preparedStatementInsert.setString(6, productItemsBO.getProperties().getName());
                preparedStatementInsert.setString(7, productItemsBO.getProperties().getShortDescription());
                preparedStatementInsert.setString(8, productItemsBO.getProperties().getOwnerPermalink());
                preparedStatementInsert.setString(9, productItemsBO.getProperties().getOwnerWebPath());
                preparedStatementInsert.setString(10, productItemsBO.getProperties().getOwnerName());
                preparedStatementInsert.setString(11, productItemsBO.getProperties().getProfileImageUrl());
                preparedStatementInsert.setString(12, productItemsBO.getProperties().getHomepageUrl());
                preparedStatementInsert.setString(13, productItemsBO.getProperties().getFacebookUrl());
                preparedStatementInsert.setString(14, productItemsBO.getProperties().getTwitterUrl());
                preparedStatementInsert.setString(15, productItemsBO.getProperties().getLinkedinUrl());
                preparedStatementInsert.setInt(16, productItemsBO.getProperties().getCreatedAt());
                preparedStatementInsert.setInt(17, productItemsBO.getProperties().getUpdatedAt());
                preparedStatementInsert.setString(18, productItemsBO.getProperties().getBlogUrl());
                preparedStatementInsert.setString(19, productItemsBO.getProperties().getGoogleUrl());
                preparedStatementInsert.setString(20, productItemsBO.getProperties().getInstagramUrl());
                preparedStatementInsert.setString(21, productItemsBO.getProperties().getPinterestUrl());
                preparedStatementInsert.addBatch();
            }

            preparedStatementInsert.executeBatch();
            con.commit();
            System.out.println("Record is inserted into Product table!");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            con.rollback();
        } finally {
            if (preparedStatementInsert != null) {
                preparedStatementInsert.close();
            }
            /*if (con != null) {
                con.close();
            }*/
        }
    }

    //4
    private void callAllJobsSP(){

        Connection con = null;
        String query = "{ call runAllJobs() }";
        CallableStatement stmt = null;
        Date startDate=null;
        Date endDate = null;

        try{
            con = dbConnection.getDatabaseConnection();
            con.setAutoCommit(false);
            startDate = new Date(System.currentTimeMillis());

            stmt = con.prepareCall(query);
            stmt.executeQuery();
            con.commit();
            endDate = new Date(System.currentTimeMillis());
            long msElapsedTime = startDate.getTime()-endDate.getTime();
            System.out.println(msElapsedTime);

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                con.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

        finally {
            if(stmt!=null){
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            /*if (con!=null){
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

}


