package libisinworker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author NaeemM
 */
public class DmtService {
    
    public String mapData(String records, String mappings, Properties dmtServiceConfig) throws IOException{
        String tempFilePath = "";

        File recordFile = new File(records);
        File mappingFile = new File(mappings);
           
        HttpClient httpclient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost("http://192.168.56.101/euInside_new/dmt.php/DataMapping/Libis/data_tansfer/Transform");

        List <NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("sourceFormat", "CAJSON"));
        params.add(new BasicNameValuePair("targetFormat", "OMJSON"));

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();        
        builder.addTextBody("sourceFormat", "CAJSON"); 
        builder.addTextBody("targetFormat", "OMJSON"); 
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
        String responseString = "";

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
    
}
