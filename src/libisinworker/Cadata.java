package libisinworker;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLEncoder;

import java.util.Properties;




/**
 *
 * @author NaeemM
 */
public class Cadata {

    public String getSetData(String setName, Properties caServerConfig, String bundle) throws MalformedURLException, IOException{
        String tempFilePath = ""                          ;
        try {
            LibisinUtil libisinUtils = new LibisinUtil();
            ClientConfig config = new DefaultClientConfig();
            Client client = Client.create(config);
            client.addFilter(new HTTPBasicAuthFilter(caServerConfig.getProperty("ca_user_id"), caServerConfig.getProperty("ca_user_password")));             
                                                
            String setSearch = "set:\""+ setName +"\"";
            String url = "http://" + caServerConfig.getProperty("ca_server") 
                    + "/" + caServerConfig.getProperty("ca_base_path") + "/" 
                    + caServerConfig.getProperty("ca_object_path") 
                    + "?q="+ URLEncoder.encode(setSearch, "UTF-8") +"&pretty=1&format=edit" ;                                       
            
            System.out.println("\nProcessing set "+ setName);
            System.out.println(url);                             
            
            WebResource webResource = client.resource(url);                        
            ClientResponse response = webResource.type("application/json").post(ClientResponse.class, bundle);

            if (response.getStatus() != 200) {
                    throw new RuntimeException("Failed : HTTP error code : "
                         + response.getStatus());
            }
                               
            String output = response.getEntity(String.class);
            tempFilePath = libisinUtils.writeTempFile("temp", setName+".json", output);

        } catch (Exception e) {
            e.printStackTrace();    
        }
        
        return tempFilePath;
    }
    
   
}
