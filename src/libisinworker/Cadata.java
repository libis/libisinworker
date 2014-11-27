package libisinworker;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;




/**
 *
 * @author NaeemM
 */
public class Cadata {
    protected Logger requestLog;

    public String getSetData(String setName, Properties caServerConfig, String bundle, String setRecordsType, String requestDirectory){
        
        LibisinUtil libisinUtils = new LibisinUtil();                
        try {
            ClientConfig config = new DefaultClientConfig();
            Client client = Client.create(config);
            client.addFilter(new HTTPBasicAuthFilter(caServerConfig.getProperty("ca_user_id"), caServerConfig.getProperty("ca_user_password")));    
          
            String endPoint = null;
            switch(setRecordsType){
                case "collecties":      
                case "collections":      
                    endPoint = caServerConfig.getProperty("ca_collection_path");
                break;                        
                    
                case "objecten":   
                case "objects":                    
                    endPoint = caServerConfig.getProperty("ca_object_path");
                break;  

                case "entiteiten":   
                case "entities":                    
                    endPoint = caServerConfig.getProperty("ca_entity_path");
                break;                      

                case "gebeurtenissen":   
                case "occurrences":                    
                    endPoint = caServerConfig.getProperty("ca_occurrence_path");
                break;                        
                                                            
                default:    /* Default endpoint is object  */
                    endPoint = caServerConfig.getProperty("ca_object_path"); 
            }                                       
            
            String setSearch = "set:\""+ setName +"\"";
            String url = "http://" + caServerConfig.getProperty("ca_server") 
                    + "/" + caServerConfig.getProperty("ca_base_path") + "/" 
                    + endPoint
                    + "?q="+ URLEncoder.encode(setSearch, "UTF-8") +"&pretty=1&format=edit" ;                                       
            
            System.out.println("-->Processing set "+ setName);                       
            this.requestLog.log(Level.INFO, "Processing set: {0}", setName);
            this.requestLog.log(Level.INFO, "Collectieve Access rest api url: {0}", url);

            WebResource webResource = client.resource(url);                        
            ClientResponse response = webResource.type("application/json").post(ClientResponse.class, bundle);
            String output = response.getEntity(String.class);

            if (response.getStatus() != 200) {
                this.requestLog.log(Level.SEVERE, "Collective Access record retrieval failed(Message): {0}", output); 
                this.requestLog.log(Level.SEVERE, "Collective Access record retrieval failed(Code): {0}", response.getStatus()); 
                return null;
            }                      
            return libisinUtils.writeFile(requestDirectory + "/" +setName+".json", output, false);   

        } catch (UnsupportedEncodingException | RuntimeException e) {
            this.requestLog.log(Level.SEVERE, "Collective Access record retrieval exception: {0}", e.getMessage()); 
            return null;
        }
        
    }
       
}
