package libisinworker;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;




/**
 *
 * @author NaeemM
 */
public class Cadata {
    protected Logger requestLog;

    public String getSetData(String setName, Properties caServerConfig, String bundle, String setRecordsType, String requestDirectory){        
        LibisinUtil libisinUtils = new LibisinUtil();                
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
            
        try {    
            String setSearch = "set:\""+ setName +"\"";
            String url = "http://" + caServerConfig.getProperty("ca_server") 
                    + "/" + caServerConfig.getProperty("ca_base_path") + "/" 
                    + endPoint
                    + "?q="+ URLEncoder.encode(setSearch, "UTF-8") +"&pretty=1&format=edit" ;                                                           

            System.out.println("-->Processing set "+ setName);                       
            this.requestLog.log(Level.INFO, "Processing set: {0}", setName);
            this.requestLog.log(Level.INFO, "Collectieve Access rest api url: {0}", url);
            this.requestLog.log(Level.INFO, "Bundle: {0}", bundle);

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
                this.requestLog.log(Level.INFO, "Downoading file with alternative method: "); 

                /*  
                    For api calls with large waiting time connection with collection access is terminated. 
                    In such cases, collective access will store the response(result) in app/tmp directory. 
                    Here we download response of our api request from app/tmp directory.
                */

                String tempFileUrl = "http://" + caServerConfig.getProperty("ca_server") 
                        + "/" + caServerConfig.getProperty("ca_base_path") + "/app/tmp/"+setName+".txt" ;  
                tempFileUrl = tempFileUrl.replace("service.php/", "");
                System.out.println("File to be downloaded from: " + tempFileUrl);     

                String tempFileStoredIn = this.downloadFromUrl(tempFileUrl, requestDirectory + "/" +setName+".json");
                return tempFileStoredIn;
            }
        
    }
    
    public String downloadFromUrl(String tempFileUrl, String localFilename){
        int numberOfTries = 1;
        int totalNulberOfTries = 20;
        try {            
            File filetoWrite = new File(localFilename);            
            URL url = new URL(tempFileUrl);
            this.requestLog.log(Level.INFO, "Downloading temp file from: {0}", url.toString());
            String filePath;
            filePath = null;
            do{
                this.requestLog.log(Level.INFO, "{0}", String.format("Downloading temp file, try number: %s/%s", numberOfTries, totalNulberOfTries));                 
                filePath = this.downloadFile(url, filetoWrite);
                if(filePath == null){
                    this.requestLog.log(Level.INFO, "Going to sleep:");
                    Thread.sleep(5 * 60 * 1000);    //sleep for 5 minutes
                    this.requestLog.log(Level.INFO, "Woke up:");                    
                }
                numberOfTries++;                
                /* 
                    Exit after 20 tries of 5 minute pause. That is, keep trying 
                    for 1 hour and 40 minutes with a 5 minute pause in between. 
                    As soon as the file is available, stop waiting and start 
                    processing records in the file.
                */
                if(numberOfTries == totalNulberOfTries) 
                    break;
                
            }while(filePath == null);

            return filePath;
            
        } catch (MalformedURLException | InterruptedException ex ) {
            this.requestLog.log(Level.SEVERE, "Collective Access record retrieval temp file download exception: {0}", ex.getMessage());
            this.requestLog.log(Level.SEVERE, "Collective Access record retrieval temp file download exception: {0}", ex.getStackTrace());
            return null;
        } 
    }
    
    public String downloadFile(URL url, File filetoWrite){
        try {
            FileUtils.copyURLToFile(url, filetoWrite);    
            if(filetoWrite.exists() && filetoWrite.length() > 0){
                this.requestLog.log(Level.INFO, "File downloaded successfully. Length {0} kb", filetoWrite.length()/1024);
                return filetoWrite.getAbsolutePath();
            }                
            else
                return null;
            
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "Collective Access record retrieval temp file not available: {0}", ex.getMessage());
            this.requestLog.log(Level.SEVERE, "Collective Access record retrieval temp file not available: {0}", ex.getStackTrace());
            return null;
        }
    }       
    
    //temp_start

    //todo: add logging
    /**
     * Get relationship types information from collective access with a rest api call.
     * @param caServerConfig
     * @param requestDirectory
     * @return
     */
        public Map<String, String> getTypes(Properties caServerConfig, String requestDirectory){
            LibisinUtil libisinUtils = new LibisinUtil();
            Map<String,String> typesMap = new HashMap<>(); 
            ClientConfig config = new DefaultClientConfig();
            Client client = Client.create(config);
            client.addFilter(new HTTPBasicAuthFilter(caServerConfig.getProperty("ca_user_id"), caServerConfig.getProperty("ca_user_password")));  

            String url = "http://" + caServerConfig.getProperty("ca_server") 
                    + "/" + caServerConfig.getProperty("ca_base_path") + "/model/ca_objects";           
            WebResource webResource = client.resource(url);                        
            ClientResponse response = webResource.type("application/json").get(ClientResponse.class);
            String output = response.getEntity(String.class);
            
            if(response.getStatus() == 200){
                try {
                    libisinUtils.writeFile(requestDirectory + "/types.json", output, false);

                    JSONParser parser = new JSONParser();            
                    JSONObject responseBodyobj = (JSONObject) parser.parse(output);
                    Set<String> keySet = responseBodyobj.keySet();
                    for(String s: keySet){
                        if(s.toLowerCase().equals("ok"))
                            continue;

                            JSONObject subObject = (JSONObject)responseBodyobj.get(s);
                            if(subObject.containsKey("relationship_types")){
                                JSONObject typeObject = (JSONObject)subObject.get("relationship_types");
                                Set<String> subKeySet = typeObject.keySet();
                                for(String subKey: subKeySet){
                                    JSONObject elementObject = (JSONObject)typeObject.get(subKey);                                
                                    Set<String> elementKeySet = elementObject.keySet();
                                    for(String elementKey: elementKeySet){
                                        JSONObject elementSubObject = (JSONObject)elementObject.get(elementKey);
                                        if(!typesMap.containsKey(elementSubObject.get("type_id").toString())){
                                            typesMap.put(elementSubObject.get("type_id").toString(), elementSubObject.get("typename").toString());                                    
                                        }                                        
                                    }                                
                                }                            
                            }                                                
                    }

                } catch (ParseException ex) {
                    this.requestLog.log(Level.SEVERE, "Collective Access relationship type information retrieval failed: {0}", ex.getMessage());
                    return null;
                }            
            }
           return typesMap;
    }    

       
}
