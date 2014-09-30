
package libisinworker;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author NaeemM
 */
public class OmekaData {
    
    private Properties omekaServerConfig;
    
    public OmekaData(Properties omekaServerConfig){
        this.omekaServerConfig = omekaServerConfig;
    }
        
    public String updateData(String records) throws IOException{
        
        try {
            JSONParser parser = new JSONParser();
            JSONArray messageBodyobj = (JSONArray) parser.parse(records);
            //System.out.println(messageBodyobj);

            for(int i=0; i< messageBodyobj.size(); i++){                
                JSONObject object = (JSONObject)messageBodyobj.get(i);
                //String responseString = this.getItem(object.get("id").toString());
                //System.out.println(object);                
                this.addItem(object);
            }                        
            return "";
        } catch (ParseException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return "true";
    }
    
    public String addItem(JSONObject object) throws IOException{
        //URI uri = this.prepareRequst("item",null);     
        HttpPost httppost = new HttpPost("http://192.168.56.101/omeka/api/items?key=808ffd70a34a749e331fc4103ce5643752c11ff4");
        HttpClient httpclient = new DefaultHttpClient();
        httppost.setHeader("Content-type", "application/json");
        httppost.setEntity(new StringEntity(object.toString()));

        HttpResponse response = httpclient.execute(httppost);       

        HttpEntity entityResponse= response.getEntity();
        String responseString = EntityUtils.toString(entityResponse, "UTF-8");
       
        System.out.println(responseString);
        System.out.println(response.getStatusLine());

            
        return "";
    }
    
    public URI prepareRequst(String apiType, String id){
        URIBuilder builder = new URIBuilder();
        URI uri = null;
        String path = null;

        builder.setScheme("http").setHost(this.omekaServerConfig.getProperty("omeka_url_base"));
        switch(apiType){
            case "item":
                path = "/"+ this.omekaServerConfig.getProperty("url_item");
                
                if(id != null){
                    path += "/" + id;                    
                }
                    
                builder.setPath(path);                
                break;

            case "resource":
                break;                
        }
        
        try {
            uri = builder.build();
        } catch (URISyntaxException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        }
        return uri;
    }
        
    public String getItem(String itemId){
        String responseString = "";
        JSONArray omekaItems = new JSONArray();
	try {                    
            URI uri = this.prepareRequst("item",itemId);     
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
