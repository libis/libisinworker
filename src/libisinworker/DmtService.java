package libisinworker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author NaeemM
 */
public class DmtService {
    public Logger requestLog;
    
    public String mapData(String records, String mappings, Properties dmtServiceConfig) throws IOException, URISyntaxException{
        String tempFilePath = "";

        File recordFile = new File(records);
        File mappingFile = new File(mappings);
        
        URIBuilder urlBuilder = new URIBuilder();
        urlBuilder.setScheme("http").setHost(dmtServiceConfig.getProperty("dmt_url_base")).setPath("/"+ dmtServiceConfig.getProperty("url_transform"));
        URI uri = urlBuilder.build();                
        HttpClient httpclient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost(uri);

        this.requestLog.log(Level.INFO, "DMT mapping request url: {0}", uri);
        
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();        
        builder.addTextBody("sourceFormat", dmtServiceConfig.getProperty("source_format")); 
        builder.addTextBody("targetFormat", dmtServiceConfig.getProperty("target_format")); 
        builder.addPart("record", new FileBody(recordFile)); 
        builder.addPart("mappingRulesFile", new FileBody(mappingFile)); 
        HttpEntity entity = builder.build();
        httppost.setEntity(entity);

        HttpResponse response = httpclient.execute(httppost);       

        HttpEntity entityResponse= response.getEntity();
        String responseString = EntityUtils.toString(entityResponse, "UTF-8");
        
        return responseString;
    }
    
    public String fetchOmekaDmt(String dmtRequestId, Properties dmtServiceConfig){
        String responseString = null;

	try {
            URIBuilder builder = new URIBuilder();
            builder.setScheme("http").setHost(dmtServiceConfig.getProperty("dmt_url_base")).setPath("/"+ dmtServiceConfig.getProperty("url_fetch_record"))
                .setParameter("request_id", dmtRequestId);
            URI uri = builder.build();                       
            HttpGet httpget = new HttpGet(uri);
            
            this.requestLog.log(Level.INFO, "DMT fetch request url: {0}", uri);
            
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget); 
            HttpEntity entity = response.getEntity();
            responseString = EntityUtils.toString(entity, "UTF-8");
            
	  } catch (URISyntaxException | IOException | ParseException e) {
                this.requestLog.log(Level.SEVERE, "DMT fetch requst exception: {0}", e.getMessage());
                return null;
	  }        
        
        return responseString;
    }
        
}
