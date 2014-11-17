package libisinworker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
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
    
    public String mapData(String records, String mappings, Properties dmtServiceConfig) throws IOException, URISyntaxException{
        String tempFilePath = "";

        File recordFile = new File(records);
        File mappingFile = new File(mappings);
        
        URIBuilder urlBuilder = new URIBuilder();
        urlBuilder.setScheme("http").setHost(dmtServiceConfig.getProperty("dmt_url_base")).setPath("/"+ dmtServiceConfig.getProperty("url_transform"));
        URI uri = urlBuilder.build();     
           
        HttpClient httpclient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost(uri);

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
            
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget); 
            HttpEntity entity = response.getEntity();
            responseString = EntityUtils.toString(entity, "UTF-8");
         
	  } catch (Exception e) {
		e.printStackTrace();
	  }        
        
        return responseString;
    }
    
    //NOTE1: If "Object" (PUT,omeka:item_type:name,"Object") is not given throw an error, if given use this as type_id. Search item types for this paticular name.
    //NOTE2: Following element should be used to check if a collectiveaccess record exists.
//    			"text":"9337",;
//			"element_set":{
//				"id":3,
//				"url":"http:\/\/192.168.56.101\/omeka\/api\/element_sets\/3",
//				"name":"Item Type Metadata",
//				"resource":"element_sets"
//			},
//			"element":{
//				"id":252,
//				"url":"http:\/\/192.168.56.101\/omeka\/api\/elements\/252",
//				"name":"object_id",
//				"resource":"elements"
//			}
    
    
}
