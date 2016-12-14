package com.ge.job.scheduler.services;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * Created by User on 13/12/2016.
 */
@Service
public class GESolrExporter {

    @Value("${solr.coreurl}")
    private String solrCoreUrl;

    public void exportToSolr(){
        SolrClient solr = new HttpSolrClient.Builder(solrCoreUrl).build();
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt","/dataimport");
        params.set("command", "full-import");
        params.set("clean", false);
        QueryResponse response = null;
        try {
            response = solr.query(params);
            response.getResults();
            response.getElapsedTime();
            System.out.println("response = " + response);
            System.out.println("Timetaken to export to Solr = " + response.getElapsedTime());

        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
            try {
                solr.rollback();
            } catch (SolrServerException e1) {
                e1.printStackTrace();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } finally {
            try {
                solr.commit();
                solr.close();
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
