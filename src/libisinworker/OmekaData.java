
package libisinworker;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
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
    
    private final Properties omekaServerConfig;
    private final LibisinUtil libisinUtils;
    
    
    public OmekaData(Properties omekaServerConfig){
        this.omekaServerConfig = omekaServerConfig;       
        this.libisinUtils = new LibisinUtil();
    }
        
    public String updateData(String records) throws IOException{
        
        try {
            JSONParser parser = new JSONParser();
            JSONArray messageBodyobj = (JSONArray) parser.parse(records);
            
            URI uri = this.prepareRequst("item", null, true);
            HttpPost httppost = new HttpPost(uri);
            HttpPut httpput = new HttpPut(uri);
            
            JSONObject elements = this.getElements(null);

            for(int i=0; i< messageBodyobj.size(); i++){ 
                JSONObject object = (JSONObject)messageBodyobj.get(i);                
                List responeList = this.processRecords(object, elements); //process each object: 
                
                String requestType = responeList.get(0).toString();
                JSONObject omekaObject = (JSONObject)responeList.get(1);

                switch(requestType){
                    case "ADD":                        
                        this.addItem(object, httppost);                        
                        break;
                        
                    case "UPDATE":
                        this.updateItem(object, httpput);
                        break;
                }                
            }                        
            return "";
        } catch (ParseException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return "true";
    }

    /**
     * Checks if an object exists. If exists, sets operation type to UPDATE otherwise to ADD.
     * Checks if elements of object exists. If an element exists, replace element 
     * and element set values with the appropriate values received from Omeka.
     * if an element does not exist, through an error for that particular element.
     * @param object object information
     * @param elements set of element id and element set id
     * @return  Type of the operation to be performed (ADD or UPDATE) and modified object
     */ 
    public  List processRecords(JSONObject object, JSONObject elements){
        List responseList = new ArrayList();
        
        if(elements.containsKey(this.omekaServerConfig.getProperty("object_identifier").replaceAll("\\s", ""))){
            responseList.add("UPDATE");
            //log
        }            
        else{
            responseList.add("ADD");
            //log
        }
        
        JSONArray objectElementsArray = (JSONArray) object.get("element_texts");                                         
        for (Object objectElement : objectElementsArray) {
            JSONObject element = (JSONObject) objectElement;
            
            String itemType = "";
            String elementName = "";
                        
            String[] parts = element.get("element").toString().split("::");
            if(parts.length > 1){
                itemType = parts[0];
                elementName = parts[1];
            }
            else
                elementName = parts[0];         
            
            String[] subParts = elementName.split(":");
            elementName = subParts[subParts.length-1];
            
            // if element exists, add element id and element set id
            if(elements.containsKey(elementName)){ 
                
                JSONArray elementArray = (JSONArray) elements.get(elementName);
                String elementId  = elementArray.get(0).toString();
                String elementSetId  = elementArray.get(1).toString();
                                
                JSONObject elementObj = new JSONObject();
                elementObj.put("id", elementId);
  

                JSONObject elementSetObj = new JSONObject();
                elementSetObj.put("id", elementSetId);
  
                element.remove("element");
                          
                element.put("element", elementObj);                  
                element.put("element_set", elementSetObj);  
                                
            }                            
            else{
                //remove element from object
                element.clear();               
                //Throw an error
                //log
            }               
        }            
        responseList.add(object);                
        return responseList;
    }
    
        
    public String addItem(JSONObject object, HttpPost httppost) throws IOException{
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
    public String updateItem(JSONObject object, HttpPut httpput){
        return "";
    }
    
    public String getItem(String itemId){
        String responseString = "";
        JSONArray omekaItems = new JSONArray();
	try {                    
            URI uri = this.prepareRequst("item",itemId, false);     
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

    /**
     * Returns element name and identifier, and identifier of the associated element set
     * @param  elementId  identifier of an element. If null, values will be returned for all elements
     * @return      element name, element id and element set id
     */    
    public JSONObject getElements(String elementId){   
        JSONObject elements = new JSONObject();
               
	try {                    
            URI uri = this.prepareRequst("element",elementId, false);     
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
           
            JSONParser parser = new JSONParser();
            JSONArray messageBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));            
            for (Object message : messageBody) {
                JSONObject objectElement = (JSONObject) message;
                JSONObject objectElementSet = (JSONObject)objectElement.get("element_set");
                
                JSONArray values = new JSONArray();
                values.add(objectElement.get("id").toString());
                values.add(objectElementSet.get("id").toString());
                elements.put(objectElement.get("name").toString().toLowerCase(), values);                
            }
            
	  } catch (Exception e) {
		e.printStackTrace();
                e.getMessage();
	  }                
        return elements;
    }    

    /**CURRENTLY NOT USED
     * Returns a pair of item type name and identifier
     * @param typeId identifier of an item type     
     * @return  returns a pair of item type id and name of the if no argument is provided
     * values of all existing item types are returned.
     */ 
    public JSONObject getItemTypes(String typeId){   
        JSONObject itemTypes = new JSONObject();
	try {                    
            URI uri = this.prepareRequst("item_type",typeId, false);     
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
           
            JSONParser parser = new JSONParser();
            JSONArray messageBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8")); 
           
            for (Object message : messageBody) {
                JSONObject objectElement = (JSONObject) message;
                
                itemTypes.put(objectElement.get("name").toString(), objectElement.get("id"));                
            }
            
	  } catch (Exception e) {
		e.printStackTrace();
                e.getMessage();
	  }                
        return itemTypes;
    }     
    
    public URI prepareRequst(String apiType, String id, boolean useKey){
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

            case "element":
                path = "/"+ this.omekaServerConfig.getProperty("url_element");
                
                if(id != null){
                    path += "/" + id;                    
                }
                    
                builder.setPath(path);                
                break;                

            case "item_type":
                path = "/"+ this.omekaServerConfig.getProperty("url_"+apiType);
                
                if(id != null){
                    path += "/" + id;                    
                }
                    
                builder.setPath(path);                
                break;                
                
            case "resource":
                break;                
        }
        
        try {
            if(useKey)
                builder.addParameter("key", this.omekaServerConfig.getProperty("key"));

            uri = builder.build();
            
        } catch (URISyntaxException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        }
        return uri;
    }
        
    
}
