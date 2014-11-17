
package libisinworker;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        
    public String updateData(String records, String setRecordsType) throws IOException{
        
        try {
            JSONParser parser = new JSONParser();
            JSONArray messageBodyobj = (JSONArray) parser.parse(records);
            String type_id = null;
            
            libisinUtils.writeTempFile("dmtomeka", ".json", records);            //TBRemoved
            JSONObject elements = this.getElements(null);
            

            System.out.println("Started at: " + new Timestamp(new java.util.Date().getTime()));
            System.out.println("Total records: " + messageBodyobj.size() + "\n");
            
            for(int i=0; i< messageBodyobj.size(); i++){ 
                JSONObject object = (JSONObject)messageBodyobj.get(i);  
                                
                Boolean isValidRecord = false;
                String urlType = null;
                switch(setRecordsType){
                    /*  For type 'collection' a record is valid if it contains value for 
                        'collection::dc:title' field. Unlike 'objects', for 'collection' type 
                        validity of all records needs to be checked individually. 
                        (For objects,entities,occurrences, if one record is invalid others are invalid too and vice versa. 
                        Because PUT command, if provided will put 'item_type' value for all records.
                        If not provided, all records will have null value for 'item_type'.)                    
                    */
                    case "collecties":      
                    case "collections":
                        urlType = "collection";
                        if(object.get("element_texts") != null){
                        JSONArray objectElementsArray = (JSONArray) object.get("element_texts");                        
                        if(this.isValidCollection(objectElementsArray))
                            isValidRecord = true;                        
                        }

                    break;                        

                    /*  For type 'objects,entities,occurrences' a record is valid if it has value for 'item_type' field.
                        Value for this field is set by PUT command (PUT,"object",items::item_type),
                        which sets item_type=object for all records. If PUT command is not used,
                        item_type value for all records will be null. Which means all records are
                        invalid, therefore no further processing is required.
                    */
                    case "objecten":      
                    case "objects":      
                    case "entities":      
                    case "entiteiten":
                    case "gebeurtenissen":
                    case "occurrences":
                        urlType = "item";
                        if(object.get("item_type") != null){
                            /*  Get type_name and type_id only once(i.e. for the first record) and reuse it
                                for next records.
                            */
                            if(type_id == null){
                                JSONObject itemTypes = this.getItemTypes(null);                                
                                if(itemTypes.get(object.get("item_type").toString()) != null)
                                {
                                    //System.out.println("Item Type ("+ object.get("item_type") +") Found : " + itemTypes.get(object.get("item_type")).toString());
                                    type_id = itemTypes.get(object.get("item_type")).toString();                               
                                    isValidRecord = true;
                                }
                                else
                                {
                                    System.out.println("Item Type ("+ object.get("item_type") +") not Found.");
                                    //log that item type was not found.
                                }                                
                            }
                            else
                                isValidRecord = true;                                                                                                                   
                        }                            
                    break;  
                        
                    default:
                        isValidRecord = false; /* By default a record is invalid.*/
                }                
                
                if(isValidRecord == false){
                    System.out.println("Invalid record. No further processing for this record.");
                    break;
                }                    
                else
                    System.out.println("Valid record.");                    

                /*  Process each record. */
                System.out.println("Processing: " + (i+1));
                List responeList = this.processRecords(object, elements, type_id); 
                
                if(responeList.size() == 2 && responeList.get(0) != null && responeList.get(1) != null){                    
                    String requestType = responeList.get(0).toString();
                    JSONObject omekaObject = (JSONObject)responeList.get(1);
                    
                    System.out.println("Record type: " + urlType);
                    System.out.println(omekaObject.toString());

                    switch(requestType){
                        case "ADD":             
                            System.out.println("Add " + urlType);
                            this.addItem(omekaObject, urlType);                        
                            break;

                        case "UPDATE":
                            System.out.println("Update " + urlType);
                            this.updateItem(omekaObject, urlType);
                            break;
                    }                      
                }
                else{
                    System.out.println("Processing failed for record number: " + i+1);                                        
                }                
                System.out.println("----------------");
            }
            
            System.out.println("Finished at: " + new Timestamp(new java.util.Date().getTime()));
            return "";
        } catch (ParseException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return "true";
    }

    public Boolean isValidCollection(JSONArray objectElementsArray){
        Boolean isValid = false;
        for (Object objectElement : objectElementsArray) {
            JSONObject element = (JSONObject) objectElement; 

            if(element.get("element").toString().equals(this.omekaServerConfig.getProperty("valid_collection")))
                isValid = true;
        }       
        return isValid;
    }
    
    
    /**
     * Checks if an object exists. If exists, sets operation type to UPDATE otherwise to ADD.
     * Checks if elements of object exists. If an element exists, replace element 
     * and element set values with the appropriate values received from Omeka.
     * if an element does not exist, through an error for that particular element.
     * @param object object information
     * @param elements set of element id and element set id
     * @param type_id
     * @return  Type of the operation to be performed (ADD or UPDATE) and modified object
     */ 
    public  List processRecords(JSONObject object, JSONObject elements, String type_id){
        List responseList = new ArrayList();
        String elementIdentifier = null;
        
        /*  Type_id has values for all non collection types. */
        if(type_id != null){
            /*  Add item_type to omeka record.  */
            JSONObject item_type = new JSONObject();
            item_type.put("id", type_id);
            object.put("item_type", item_type); 
            
            //get identifier element id, by using type_id and identifier name (in this case 'object_id')
            //this identifier will be used later(when processing object_id element) to see if an object
            //with this identifier exists.
            JSONObject elementObject = this.getElementByName(type_id, this.omekaServerConfig.getProperty("object_identifier"));
            if(!elementObject.isEmpty() && elementObject.get("id") !=null){
                    elementIdentifier = elementObject.get("id").toString();                                        
            }
            else{
                //log and return
                return null;            
            }                
        }
        
        JSONArray objectElementsArray = (JSONArray) object.get("element_texts");                                         
        for (Object objectElement : objectElementsArray) {
            JSONObject element = (JSONObject) objectElement;
            
            String itemType = "";
            String elementName = "";
            
            String elementIdToAdd = null;
            String elementSetIdToAdd = null;
                        
            String[] parts = element.get("element").toString().split("::");
            
            if(parts.length > 1){
                itemType = parts[0];
                elementName = parts[1];
            }
            else
                elementName = parts[0];         
            
            
            /// find elements from correct name space. Element name should contain namespace, that is ':'.
            if(elementName.contains(":")){
                String[] splitName = elementName.split(":"); 
                if(this.omekaServerConfig.getProperty(splitName[0]) != null && !splitName[1].equals(this.omekaServerConfig.getProperty("object_identifier"))){
                    JSONObject elementSetObject = this.getElementSetByName(this.omekaServerConfig.getProperty(splitName[0]));
                    
                    if(elementSetObject.get("id") != null){
                        String elementToFind = splitName[1]= splitName[1].substring(0,1).toUpperCase() + splitName[1].substring(1).toLowerCase();
                        
                        JSONObject foundElement = this.getElementInSet(elementToFind, elementSetObject.get("id").toString());
                        if(foundElement != null){
                            elementIdToAdd = foundElement.get("id").toString();
                            elementSetIdToAdd = elementSetObject.get("id").toString();
                        }
                        else{
                            //System.out.println(elementToFind + " element not found in set " + elementSetObject.get("id").toString());
                        }
                    }
                    else{
                        //do not add this element, move to the next element
                        //System.out.println("element set not found:("+ this.omekaServerConfig.getProperty(splitName[0]) +")");
                        
                    }                    
                }
                else{
                    //System.out.println("No corresponding long name defined in configuration for element set " + splitName[0]);
                    //do not add this element, move to the next element
                }                    
            }
            else{
                //System.out.println("Skipped => " + elementName + ". No name space provided.");    
            }

            ///            
            
            
            
            
            String[] subParts = elementName.split(":");                        
            elementName = subParts[subParts.length-1];


            
            // Note: 'spatial coverage' element additionally needs to be linked with 'Geolocation' plugin
            //'digitoolurl' element should be added separately by using the api created by Joris
            
            // if element exists, add element id and element set id
            // Note: replace this part with Switch
            if(elements.containsKey(elementName)){ 
                
                JSONArray elementArray = (JSONArray) elements.get(elementName);                
                String elementId  = elementArray.get(0).toString();
                String elementSetId  = elementArray.get(1).toString();
                
                
                
                // If non collection record and object_id is available in the record received from dmt service
                if(type_id != null && elementName.equals(this.omekaServerConfig.getProperty("object_identifier"))){
                    List existResponseList = this.recordExists(elementIdentifier, element.get("text").toString());
                    // existResponseList.get(0) contains whether record exists in omeka or not.
                    // Possible values are: true, false or null. 
                    // true: if a record is found
                    // false: no record found
                    // null: find record request to omeka was unsuccessfull
                    
                    // existResponseList.get(1) contains identifier of the found record, 
                    // which is needed to make update requst
                    if(existResponseList.get(0) != null){
                        //record found therefore it is an upate operation
                        if((Boolean)existResponseList.get(0) == true){
                            responseList.add("UPDATE"); 
                            object.put("id", existResponseList.get(1));
                        }   
                                                    
                        //record not found therefore it is an add operation
                        if((Boolean)existResponseList.get(0) == false)   
                            responseList.add("ADD"); 

                    }                        
                    else        // null value, therefore no further processing for this object.                       
                    {
                        //Log: something went wrong while verifying if object exists in omeka, 
                        //it did not return tur or false, infact it returned null;                        
                        break;
                    }
                }
                
                if(type_id == null && elementName.toLowerCase().equals("title")){    // It is collection type
                    //Check if collection exists:
                    //1. if multiple collection exists, do not proceed, throw an error
                    //2. if one collection exists, perform update operation
                    //3. if no collection exists, perform add operation
                    
                    List collectionExistResponseList = this.collectionExists(element.get("text").toString());
                    if(collectionExistResponseList.get(0) != null){
                        //collection found therefore it is an upate operation
                        if((Boolean)collectionExistResponseList.get(0) == true){
                            responseList.add("UPDATE"); 
                            object.put("id", collectionExistResponseList.get(1));
                            System.out.println(element.get("text").toString() + " - collection found, it is an update operation");
                        } 
                        
                        //collection not found therefore it is an add operation
                        if((Boolean)collectionExistResponseList.get(0) == false){
                            System.out.println(element.get("text").toString() + " - collection not found, it is an add operation");
                            responseList.add("ADD");                         
                        }   
                            
                    }
                    else{
                        System.out.println("Collection (" + element.get("text").toString() + ") exist check faild.");
                        break;
                    }
                
                }
                

                JSONObject elementObj = new JSONObject();
                elementObj.put("id", elementId);
                //elementObj.put("id", elementIdToAdd);


                JSONObject elementSetObj = new JSONObject();
                elementSetObj.put("id", elementSetId);
                //elementSetObj.put("id", elementSetIdToAdd);

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
    
    public List collectionExists(String collectionTitle){   
        List responseList = new ArrayList();
        Boolean collectionExists = null;
        String collectionIdentifier = null;     
        JSONArray collectionsArray = (JSONArray)this.getCollections(null);
        if(collectionsArray == null)
            return null;
        
        collectionExists = false;   // Before searching we assume that collection does not exist.
        int counter = 0;
        for (Object collectionObject : collectionsArray) {
            JSONObject collectionObj = (JSONObject) collectionObject;
            
            for (Object collectionElements : (JSONArray) collectionObj.get("element_texts")) {
                JSONObject collectionElement = (JSONObject) collectionElements;
                if(collectionElement.get("text").toString().equals(collectionTitle)){
                    collectionExists = true;
                    collectionIdentifier = collectionObj.get("id").toString();
                    counter++;
                }                    
            }
        }
        
        // If multiple collections found, it is an error.
        if(collectionExists == true && counter > 1)
            collectionExists = null;
        
        responseList.add(collectionExists);
        responseList.add(collectionIdentifier);
        return responseList;        
    }

    /**
     * Returns collections
     * @param  collectionId  identifier of a collection. If null, values will be returned for all collections
     * @return      collection(s)
     */    
    public JSONArray getCollections(String collectionId){   
        JSONArray responseBody = new JSONArray();
               
	try {                    
            URI uri = this.prepareRequst("collection",collectionId, false, null);             
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
                       
            JSONParser parser = new JSONParser();
            responseBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));  
	  } catch (Exception e) {
		e.printStackTrace();
                e.getMessage();
	  }    
        return responseBody;
    }      
    
    public List recordExists(String elementId, String recordId){                      
        List responseList = new ArrayList();
        Boolean recordExists = null;
        String recordIdentifier = null;
                
        Map<String,String> quereyParameters = new HashMap<>();
        quereyParameters.put("search", "");
        quereyParameters.put("advanced[0][element_id]", elementId);
        quereyParameters.put("advanced[0][terms]", recordId);
        quereyParameters.put("advanced[0][type]", "is exactly");
        quereyParameters.put("output", "json");
        
	try {                    
            URI uri = this.prepareRequst("item_browse",null, false, quereyParameters);                         
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
            JSONParser parser = new JSONParser();
            JSONObject messageBody = (JSONObject) parser.parse(EntityUtils.toString(entity, "UTF-8"));

            //Note: Browsing items gives only public items, that means for items with private settings we will not
            //be able to find if item exists or not. 
            if(messageBody.get("items") != null){
                int counter = 0;
                // omeka returns 'items' in json format, we assume it is an empty json, that is item does not exist.
                // if json is not empty, later itemExists will be set to true.
                recordExists = false; 
                
                JSONArray existedItems = (JSONArray) messageBody.get("items");            
                for (Object item : existedItems) {
                    if(counter > 0)
                        break;
                    JSONObject objectElement = (JSONObject) item;
                     if(objectElement.get("id") != null){
                         recordIdentifier = objectElement.get("id").toString();
                         recordExists = true;
                         counter ++;   
                     }
                }                    
            }            
                                                
	  } catch (Exception e) {
		e.printStackTrace();
                e.getMessage();
	  }
        
        responseList.add(recordExists);
        responseList.add(recordIdentifier);
        return responseList;
    }

    public JSONObject getElementByName(String itemType, String elementName){ 
        JSONObject element = new JSONObject();
        Map<String,String> quereyParameters = new HashMap<>();
        quereyParameters.put("item_type", itemType);
        quereyParameters.put("name", elementName);

	try {                    
            URI uri = this.prepareRequst("element",null, false, quereyParameters); 
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
                       
            JSONParser parser = new JSONParser();
            JSONArray messageBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));
            int counter = 0;
            for (Object message : messageBody) {
                element = (JSONObject) message;                
                if(counter == 0) //exit after getting first element
                    break;
            }
            
	  } catch (Exception e) {
		e.printStackTrace();
                e.getMessage();
	  }         
                                
        return element;
    }    
    
    public String addItem(JSONObject object, String urlType) throws IOException{
        if(urlType == null){
            System.out.println("Url type not provided. Add item quite.");
            return null;
        }
        URI uri = this.prepareRequst(urlType, null, true, null);
        HttpPost httppost = new HttpPost(uri);
        System.out.println(uri);
        
        HttpClient httpclient = new DefaultHttpClient();
        httppost.setHeader("Content-type", "application/json");
        httppost.setEntity(new StringEntity(object.toString()));

        HttpResponse response = httpclient.execute(httppost);       

        HttpEntity entityResponse= response.getEntity();
        String responseString = EntityUtils.toString(entityResponse, "UTF-8");
       
        System.out.println(responseString);
           
        return "";
    }
    public String updateItem(JSONObject object, String urlType){
        if(urlType == null){
            System.out.println("Url type not provided. Update item quite.");
            return null;
        }        
        try {
            URI uri = this.prepareRequst(urlType, object.get("id").toString(), true, null);
            System.out.println(uri);
            HttpPut httpput = new HttpPut(uri);

            HttpClient httpclient = new DefaultHttpClient();
            httpput.setHeader("Content-type", "application/json");
            httpput.setEntity(new StringEntity(object.toString()));
            
            HttpResponse response = httpclient.execute(httpput);
            
            HttpEntity entityResponse= response.getEntity();
            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
                        
            System.out.println(responseString);
                        
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }
    
    public String getItem(String itemId){
        String responseString = "";
        JSONArray omekaItems = new JSONArray();
	try {                    
            URI uri = this.prepareRequst("item",itemId, false, null);     
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
            URI uri = this.prepareRequst("element",elementId, false, null);     
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
                values.add(objectElement.get("id").toString());               // Element id      
                values.add(objectElementSet.get("id").toString());            // Element set id    
                elements.put(objectElement.get("name").toString().toLowerCase(), values);                
            }
            
	  } catch (Exception e) {
		e.printStackTrace();
                e.getMessage();
	  }                
        return elements;
    }    
    
    
    /**
     * Returns   element set
     * @param    elementSetName name of the element set
     * @return  element set in JSONObject format, if not found return null
     */    
    public JSONObject getElementSetByName(String elementSetName){          
        JSONArray responseBody = (JSONArray) this.getResources("element_set", null, null);
        
        if(responseBody == null)    //do not proceed if no element sets found
            return null;
        
        for (Object responseObject : responseBody) {
            JSONObject elementSet = (JSONObject) responseObject;
            if(elementSet.get("name").equals(elementSetName)){
                return elementSet;
            }                
        }
               
        return null;
    }     

    /**
     *
     * @param elementSetId  identifier of the set for which elements to get
     * @return elements belonging to a set
     */
    public JSONArray getElementsBySetId(String elementSetId){   
        Map<String,String> quereyParameters = new HashMap<>();
        quereyParameters.put("element_set", elementSetId);        
        return this.getResources("element", null, quereyParameters);
    }      

    /**
     *
     * @param elementName   name of the element to search
     * @param elementSetId  identifier of the set where to find element
     * @return  returns element in JSONObject format if found, otherwise returns null
     */
    public JSONObject getElementInSet(String elementName, String elementSetId){ 

        JSONArray elementObjects  = this.getElementsBySetId(elementSetId);
        
        if(elementObjects == null)
            return null;
        
        for (Object elementObject : elementObjects) {
            JSONObject element= (JSONObject) elementObject;
            if(element.get("name").equals(elementName))
                return element;
        }        
                
        return null;
    }    
    
    
    /**
     * Returns  requested resource(s)
     * @param resourceType type of the resource to get, e.g. collection,elements,element sets
     * @param id identifier of the resource, if not provided all records of that resource type will be returned
     * @param quereyParameters  query parameters to add to the url
     * @return  resource(s) in JSONArray
     */    
    public JSONArray getResources(String resourceType, String id, Map<String,String> quereyParameters){   
        JSONArray responseBody = new JSONArray();
               
	try {                    
            URI uri = this.prepareRequst(resourceType,id, false, quereyParameters);             
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget);             
            HttpEntity entity = response.getEntity();
                       
            JSONParser parser = new JSONParser();
            responseBody = (JSONArray) parser.parse(EntityUtils.toString(entity, "UTF-8"));  
	  } catch (Exception e) {
		e.printStackTrace();
                e.getMessage();
	  }    
        return responseBody;
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
            URI uri = this.prepareRequst("item_type",typeId, false, null);     
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
    
    public URI prepareRequst(String apiType, String id, boolean useKey, Map<String,String> quereyParameters){
        URIBuilder builder = new URIBuilder();
        URI uri = null;
        String path = null;

        builder.setScheme("http").setHost(this.omekaServerConfig.getProperty("omeka_url_base"));
        switch(apiType){
            case "item":
                path = "/"+ this.omekaServerConfig.getProperty("url_item");               
                break;

            case "item_browse":
                //Different base url for browse
                builder.setHost(this.omekaServerConfig.getProperty("omeka_url_browse_base"));                   
                path = "/"+ this.omekaServerConfig.getProperty("url_item_browse");              
                break;                

            case "element":
                path = "/"+ this.omekaServerConfig.getProperty("url_element");              
                break; 
                
            case "element_set":
                path = "/"+ this.omekaServerConfig.getProperty("url_element_set");              
                break; 
                
            case "item_type":
                path = "/"+ this.omekaServerConfig.getProperty("url_"+apiType);               
                break;                
                
            case "collection":
                path = "/"+ this.omekaServerConfig.getProperty("url_collection");                
                break;                       
        }
        
        try {
            
            if(id != null){
                path += "/" + id;                    
            }
            builder.setPath(path);                
                      
            if(useKey)
                builder.addParameter("key", this.omekaServerConfig.getProperty("key"));
            
            if(quereyParameters != null){
                for(String key : quereyParameters.keySet()){
                    builder.addParameter(key, quereyParameters.get(key));
                }
            }

            
            uri = builder.build();
            
            
        } catch (URISyntaxException ex) {
            Logger.getLogger(OmekaData.class.getName()).log(Level.SEVERE, null, ex);
        }
        return uri;
    }
        
    
}
