
package libisinworker;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
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
    //private GeoLocation location;
    private List<GeoLocation> locations;
    private List<DigiTool> digitoolimages;
    public  Logger requestLog;
    public int totalRecords;
    public int addedRecords;
    public int updatedRecords;
    public int failedRecords;
    public int validRecords;
    public int invalidRecords;
    
    
    public OmekaData(Properties omekaServerConfig){
        this.omekaServerConfig = omekaServerConfig;       
        this.libisinUtils = new LibisinUtil();
        //this.location = new GeoLocation();
        this.locations = new ArrayList<>();
        this.digitoolimages = new ArrayList<>();
    }
        
    public boolean pushDataToOmeka(String records, String setRecordsType, String requestDirectory, String setName) throws IOException{
        
        try {                       
            JSONParser parser = new JSONParser();                       
            JSONArray messageBodyobj = (JSONArray) parser.parse(records);
            String type_id = null;
            
            libisinUtils.writeFile(requestDirectory + "/" + setName + "_dmtoutput.json",records, false);            
            
            this.totalRecords = messageBodyobj.size();
            this.validRecords = 0;
            this.invalidRecords = 0;
            this.addedRecords = 0;
            this.updatedRecords = 0;
            this.failedRecords = 0;
            
            //JSONObject elements = this.getElements(null);       ***************//WHEN REPLACED WITH NEW DESIGN, THIS WILL NOT BE NEEDED

            this.requestLog.log(Level.INFO, "Total records: {0}", messageBodyobj.size());
            System.out.println("-->Total records: " + messageBodyobj.size());
            
            for(int i=0; i< messageBodyobj.size(); i++){ 
                System.out.println("--->Processing record: " + (i+1));
                this.requestLog.log(Level.INFO, "Processing record: {0}", (i+1));
                JSONObject object = (JSONObject)messageBodyobj.get(i);  
                                
                Boolean isValidRecord = false;
                String urlType = null;
                this.requestLog.log(Level.INFO, "record Type: {0}", setRecordsType);                
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
                                    this.requestLog.log(Level.INFO, "Item type: '{0}' Found", object.get("item_type")); 
                                    this.requestLog.log(Level.INFO, "Item type id: {0}", itemTypes.get(object.get("item_type")).toString()); 
                                    type_id = itemTypes.get(object.get("item_type")).toString();                               
                                    isValidRecord = true;
                                }
                                else
                                    this.requestLog.log(Level.INFO, "Item type( {0}) not Found", object.get("item_type"));                             
                            }
                            else
                                isValidRecord = true;                                                                                                                   
                        }                            
                    break;  
                        
                    default:
                        isValidRecord = false; /* By default a record is invalid.*/
                }                
                
                if(isValidRecord == false){
                    this.requestLog.log(Level.SEVERE, "Invalid record. No further processing for this record");
                    System.out.println("---->Invalid record.");    
                    this.invalidRecords ++;
                    break;
                }                    
                else{
                    System.out.println("---->Valid record.");
                    this.requestLog.log(Level.SEVERE, "Valid record");
                    this.validRecords ++;
                }
                    

                /*  Process each record. */
                this.requestLog.log(Level.INFO, "Record type: {0}", urlType);
                
                List responeList = this.processRecords(object, type_id);                 
                if(responeList.size() == 2 && responeList.get(0) != null && responeList.get(1) != null){                    
                    String requestType = responeList.get(0).toString();
                    JSONObject omekaObject = (JSONObject)responeList.get(1);
                                         
                    this.requestLog.log(Level.INFO, "Omeka operation type: {0}", requestType);
                    
                    boolean success = false;
                    switch(requestType){
                        case "ADD":             
                            System.out.println("---->Omeka Operation type: Add");
                            success = this.addItem(omekaObject, urlType);                        
                            break;

                        case "UPDATE":
                            System.out.println("---->Omeka Operation type: Update");
                            success = this.updateItem(omekaObject, urlType);                                
                            break;
                    } 
                    
                    if(success == true){
                        System.out.println("---->"+ requestType +" successful");  
                        this.requestLog.log(Level.INFO, "--{0} successful", requestType);
                        if(requestType.equals("ADD"))
                            this.addedRecords ++;
                        if(requestType.equals("UPDATE"))
                            this.updatedRecords ++;
                        
                    }
                    else{
                        System.out.println("---->"+ requestType +" failed");  
                        this.requestLog.log(Level.SEVERE, "--{0} failed", requestType);
                        this.failedRecords ++;
                    }                    
                    
                }
                else{
                    System.out.println("Processing failed for record number: " + i+1);
                    this.requestLog.log(Level.SEVERE, "Processing failed for record number: {0}", (i+1)); 
                }                

            }
            return true;    
        } catch (ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Omeka record processing failed"); 
            this.requestLog.log(Level.SEVERE, "Exception Message: {0}", ex.getMessage());
            return false;
        }                
    }

    public Boolean isValidCollection(JSONArray objectElementsArray){
        Boolean isValid = false;
        for (Object objectElement : objectElementsArray) {
            JSONObject element = (JSONObject) objectElement; 
            if(element.get("element").toString().trim().equals(this.omekaServerConfig.getProperty("valid_collection").trim()))
                isValid = true;
        }       
        return isValid;
    }
    
    
    /**
     * Checks if an object exists. If exists, sets operation type to UPDATE otherwise to ADD.
     * Checks if elements of object exists. If an element exists, replace element 
     * and element set values with the appropriate values received from Omeka.
     * if an element does not exist, throw an error for that particular element.
     * @param object object information
     * @param type_id
     * @return  Type of the operation to be performed (ADD or UPDATE) and modified object
     */ 
    public  List processRecords(JSONObject object, String type_id){
        List responseList = new ArrayList();
        String objectIdentifier = libisinUtils.capitalizeFirstLetter(this.omekaServerConfig.getProperty("object_identifier"));
        String digitToolElement = libisinUtils.capitalizeFirstLetter(this.omekaServerConfig.getProperty("digitoolelement"));
        /*  Type_id has values for all non collection types. */
        if(type_id != null){
            /*  Add item_type to omeka record.  */
            JSONObject item_type = new JSONObject();
            item_type.put("id", type_id);
            object.put("item_type", item_type); 
            
            //get identifier element id, by using type_id and identifier name (in this case 'object_id')
            //this identifier will be used later(when processing object_id element) to see if an object
            //with this identifier exists.
            //JSONObject elementObject = this.getElementByName(type_id, this.omekaServerConfig.getProperty("object_identifier"));
            JSONObject elementObject = this.getElementByName(type_id, objectIdentifier);
            if(!elementObject.isEmpty() && elementObject.get("id") !=null){
                this.requestLog.log(Level.INFO, "Type id found. {0}", elementObject.get("id").toString()); 
            }
            else{
                System.out.println("Element "+this.omekaServerConfig.getProperty("object_identifier")+
                        " for type "+ type_id+" is not found.");   
                this.requestLog.log(Level.SEVERE, "Type id not found, therefore no further processing for this record"); 
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
                                    
            /* Find elements from correct name space. Element name should contain namespace, that is ':'. */
            if(elementName.contains(":")){
                String[] splitName = elementName.split(":");  

                /* Set geolocation values. These values will be used later once object has been added or updated. */
                if(splitName[0].equals("geolocation") && libisinUtils.capitalizeFirstLetter(splitName[1]).equals("Address")){
                    if(element.get("text") != null)                                                                             
                        this.setGeoLocation(element.get("text").toString());
                    else          
                        this.requestLog.log(Level.SEVERE, "Geolocation value not given."); 
                    
                    element.clear();
                    continue;
                }                
                                                                                 
                if(this.omekaServerConfig.getProperty(splitName[0]) != null ){ 
                    JSONObject elementSetObject = this.getElementSetByName(this.omekaServerConfig.getProperty(splitName[0]));                                        
                                       
                    if(elementSetObject.get("id") != null){                                                
                        String elementToFind = libisinUtils.capitalizeFirstLetter(splitName[1]);                                                
                       
                            /* Digitool urls */
                        if(elementToFind.equals(digitToolElement))
                        {
                            this.requestLog.log(Level.INFO, "Processing digitool"); 
                            if(element.get("text") != null)                                                                             
                                this.setDigiToolPid(element.get("text").toString());
                            else          
                                this.requestLog.log(Level.SEVERE, "Digitool pid not given"); 

                            element.clear();
                            continue;                                
                        }
                        
                        JSONObject foundElement = this.getElementInSet(elementToFind, elementSetObject.get("id").toString());
                        if(foundElement != null){
                            elementIdToAdd = foundElement.get("id").toString();
                            elementSetIdToAdd = elementSetObject.get("id").toString();
                            this.requestLog.log(Level.INFO, "{0}", String.format("Element: '%s' found, Element id: %s, Element set id: %s", elementToFind, elementIdToAdd, elementSetIdToAdd));                            
                                                                                                                
                            // If non collection record and object_id is available in the record received from dmt service                            
                            if(type_id != null && elementToFind.equals(objectIdentifier)){
                                List existResponseList = this.recordExists(elementIdToAdd, element.get("text").toString());
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
                                    this.requestLog.log(Level.SEVERE, "Existing item check faild, no further processing for this item");                                    
                                    //it did not return tur or false, infact it returned null;                        
                                    break;
                                }
                            }    
                            
                            if(type_id == null && elementToFind.toLowerCase().equals("title")){    // It is collection type
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
                                                        
                            /*
                             * Replace element name with: element_id and element_set_id. This is done by adding a new json entry and removing the old entry with 
                               element name
                            */                             
                            JSONObject elementObj = new JSONObject();
                            JSONObject elementSetObj = new JSONObject();
                            
                            elementObj.put("id", elementIdToAdd);
                            elementSetObj.put("id", elementSetIdToAdd);
                            element.remove("element");
                            element.put("element", elementObj);                  
                            element.put("element_set", elementSetObj);
                        }
                        else{
                            this.requestLog.log(Level.SEVERE, "{0}", String.format("%s element not found in set %s", elementToFind, elementSetObject.get("id").toString())); 
                            element.clear();
                        }
                            
                    }
                    else{
                        this.requestLog.log(Level.SEVERE, "Element set not found: {0}", this.omekaServerConfig.getProperty(splitName[0]));                        
                        element.clear();
                    }
                        
                }
                else{
                    this.requestLog.log(Level.SEVERE, "No corresponding long name defined in configuration for element set: {0}", splitName[0]);                        
                    element.clear();
                }
                    
            }
            else{
                this.requestLog.log(Level.SEVERE, "{0}", String.format("No name space provided: %s", elementName)); 
                element.clear();
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
                if(collectionElement.get("text").toString().trim().equals(collectionTitle.trim())){
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
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
              this.requestLog.log(Level.SEVERE, "Getting collections from omeka failed: {0}", ex.getMessage());   
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
                                                
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
		this.requestLog.log(Level.SEVERE, "Record exists check failed: {0}", ex.getMessage());  
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
            
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
		this.requestLog.log(Level.SEVERE, "Getting elements from omeka failed: {0}", ex.getMessage());  
	  }         
                                
        return element;
    }    
    
    public boolean addItem(JSONObject object, String urlType){
        if(urlType == null){
            System.out.println("Url type not provided. Add item quite.");
            this.requestLog.log(Level.SEVERE, "Url type not provided. Add item quite");
            return false;
        }        
        
        try {
            URI uri = this.prepareRequst(urlType, null, true, null);
            HttpPost httppost = new HttpPost(uri);
            HttpClient httpclient = new DefaultHttpClient();
            httppost.setHeader("Content-type", "application/json");
            httppost.setEntity(new StringEntity(object.toString()));
            
            this.requestLog.log(Level.INFO, "Add operation url: {0}", uri);
            
            HttpResponse response = httpclient.execute(httppost);
            HttpEntity entityResponse= response.getEntity();
            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
            
            this.requestLog.log(Level.INFO, "Add operation response: {0}", responseString);
  
            if(this.locations.size() > 0){
                this.requestLog.log(Level.INFO, "Addig locations");                 
                this.addLocation(responseString);                
            }
            
            if(this.digitoolimages.size() > 0){
                this.requestLog.log(Level.INFO, "Addig images"); 
                this.addDigiToolImage(responseString);                                
            }            

            return true;
            
        } catch ( UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "Add item unsupported encoding Exception: {0}", ex.getMessage());
            return false;
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "Add item IO Exception: {0}", ex.getMessage());
            return false;
        }
    }
    
    public boolean updateItem(JSONObject object, String urlType){
        if(urlType == null){
            System.out.println("Url type not provided. Update item quite.");
            this.requestLog.log(Level.SEVERE, "Url type not provided. Update item quite");
            return false;
        }        
        try {
            URI uri = this.prepareRequst(urlType, object.get("id").toString(), true, null);                                    
            HttpPut httpput = new HttpPut(uri);
            HttpClient httpclient = new DefaultHttpClient();
            httpput.setHeader("Content-type", "application/json");
            httpput.setEntity(new StringEntity(object.toString()));
            
            this.requestLog.log(Level.INFO, "--Update operation url: {0}", uri);
            this.requestLog.log(Level.INFO, "--Update operation request Body: {0}", object.toString());
            
            HttpResponse response = httpclient.execute(httpput);                               
            HttpEntity entityResponse= response.getEntity();
            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
            this.requestLog.log(Level.INFO, "--Update operation response: {0}", responseString);            
            if(response.getStatusLine().getStatusCode() != 200){
                System.out.println("---->Update operation failed: " + responseString);
                this.requestLog.log(Level.SEVERE, "--Update operation failed(Message): {0}", responseString);
                this.requestLog.log(Level.SEVERE, "--Update operation failed(Code): {0}", response.getStatusLine().getStatusCode());
                return false;
            }            
            
            if(this.locations.size() > 0){
                this.requestLog.log(Level.INFO, "Addig locations"); 
                this.addLocation(responseString);                
            }
            
            if(this.digitoolimages.size() > 0){
                this.requestLog.log(Level.INFO, "Addig images"); 
                this.addDigiToolImage(responseString);                                
            }
                          
            return true;
            
        } catch (UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "--Update item unsupported encoding Exception: {0}", ex.getMessage());
            return false;
            
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "--Update item IO Exception: {0}", ex.getMessage());
            return false;
        }
    }
    
    public String getItem(String itemId){
        String responseString = "";        
	try {                    
            URI uri = this.prepareRequst("item",itemId, false, null);     
            HttpGet httpget = new HttpGet(uri);
            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(httpget); 
            
            HttpEntity entity = response.getEntity();
            responseString = EntityUtils.toString(entity, "UTF-8");
            
	  } catch (IOException | org.apache.http.ParseException ex) {
		this.requestLog.log(Level.SEVERE, "Get item Exception: {0}", ex.getMessage());
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
            
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
                this.requestLog.log(Level.SEVERE, "Get elements Exception: {0}", ex.getMessage());
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
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
                this.requestLog.log(Level.SEVERE, "Get resources Exception: {0}", ex.getMessage());
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
            
	  } catch (IOException | org.apache.http.ParseException | ParseException ex) {
                this.requestLog.log(Level.SEVERE, "Get resources Exception: {0}", ex.getMessage());
	  }                
        return itemTypes;
    }     
    
    public HttpResponse addResource(String urlType, String id, boolean useKey, Map<String,String> quereyParameters, String requestBody, String contentType){
        if(urlType == null){
            System.out.println("Url type not provided. Add resource quite.");
            this.requestLog.log(Level.SEVERE, "Url type not provided. Add resource quite");
            return null;
        }        
        
        try {
            URI uri = this.prepareRequst(urlType, id, useKey, quereyParameters);
            HttpPost httppost = new HttpPost(uri);
            HttpClient httpclient = new DefaultHttpClient();
            if(requestBody !=null && contentType != null){
                httppost.setHeader("Content-type", contentType);
                httppost.setEntity(new StringEntity(requestBody)); 
                this.requestLog.log(Level.INFO, "Add resource request body: {0}", requestBody);
            }            
            this.requestLog.log(Level.INFO, "Add resource url: {0}", uri);
            
            return httpclient.execute(httppost);
            
        } catch ( UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "Add resource unsupported encoding Exception: {0}", ex.getMessage());
            return null;
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "Add resource IO Exception: {0}", ex.getMessage());
            return null;
        }
    }
        
    public void addLocation(String item){
        try {
            JSONParser parser = new JSONParser();        
            JSONObject itemObj = (JSONObject) parser.parse(item);
            if(itemObj.get("id") != null){    
                System.out.println("---->Addig locations for: " + itemObj.get("id"));
                for (GeoLocation geoLocation : this.locations) {
                    JSONObject locationObj = new JSONObject();
                    JSONObject itemObject = new JSONObject();
                    itemObject.put("id", itemObj.get("id"));                     
                                        
                    if(geoLocation.latitude != null)
                        locationObj.put("latitude", geoLocation.latitude);
                    if(geoLocation.longitude != null)
                        locationObj.put("longitude", geoLocation.longitude);
                    if(geoLocation.address != null)
                        locationObj.put("address", geoLocation.address);                
                    if(geoLocation.mapType != null)
                        locationObj.put("map_type", geoLocation.mapType);
                    if(geoLocation.zoomLevel >= 0)
                        locationObj.put("zoom_level", geoLocation.zoomLevel);

                    locationObj.put("item", itemObject);

                    HttpResponse response = this.addResource("geo_location", null, false, null, locationObj.toString(), "application/json");
                    if(response != null){
                        HttpEntity entityResponse= response.getEntity();
                        String responseString = EntityUtils.toString(entityResponse, "UTF-8");
                        this.requestLog.log(Level.INFO, "Add location response: {0}", responseString);                                    

                        if(response.getStatusLine().getStatusCode() != 201){
                            this.requestLog.log(Level.SEVERE, "--Add location faild (Message): {0}", responseString);
                            this.requestLog.log(Level.SEVERE, "--Add location faild(Code): {0}", response.getStatusLine().getStatusCode());
                            System.out.println("---->Add location faild: " + responseString);
                        }
                        else
                            this.requestLog.log(Level.SEVERE, "Location addedd successfully: {0}", responseString);                        
                    }
                    else
                        this.requestLog.log(Level.INFO, "Add location faild. Error in POST request to omeka geolocation");                
                }                
            }
            else
                this.requestLog.log(Level.SEVERE, "Adding location failed. Item id missing.");    
            
        } catch (org.apache.http.ParseException | ParseException | UnsupportedEncodingException ex) {
            this.requestLog.log(Level.SEVERE, "Adding location exception: {0}", ex.getMessage());
            this.locations.clear();
        } catch (IOException ex) {
            this.requestLog.log(Level.SEVERE, "Adding location IO exception: {0}", ex.getMessage());
            this.locations.clear();
        } 
        
        this.locations.clear();
    }
        
    public void setGeoLocation(String locationString){
        
        try {
            JSONParser parser = new JSONParser();        
            JSONArray locationArray = (JSONArray) parser.parse(locationString);
            int counter = 1;
            for (Object objectItem : locationArray) {
                JSONObject geoLocation = (JSONObject) objectItem;
                if(geoLocation.get("georeference") == null)
                    continue;
                GeoLocation tempLocation = new GeoLocation();
                JSONObject locationItem = (JSONObject)geoLocation.get("georeference");
                                
                if(locationItem.get("label") != null)
                    tempLocation.address = locationItem.get("label").toString();
                if(locationItem.get("latitude") != null)
                    tempLocation.latitude = locationItem.get("latitude").toString();
                if(locationItem.get("longitude") != null)
                    tempLocation.longitude = locationItem.get("longitude").toString();
                
                if(tempLocation.address != null || tempLocation.latitude != null || tempLocation.longitude != null)
                    this.locations.add(tempLocation);

                tempLocation = null;                
                counter ++;
            }
            
        } catch (ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Parse geolocation exception: {0}", ex.getMessage());
        }
    }    
    
    public void addDigiToolImage(String item){
        try {        
            JSONParser parser = new JSONParser();
            JSONObject itemObj = (JSONObject) parser.parse(item);
            if(itemObj.get("id") != null){  
                System.out.println("---->Addig image for: " + itemObj.get("id"));
                for (DigiTool digiToolImage : this.digitoolimages) {                    
                    JSONObject imageObj = new JSONObject();
                    imageObj.put("item_id", itemObj.get("id"));
                    
                    if(digiToolImage.pid != null)
                        imageObj.put("pid", digiToolImage.pid);
                    if(digiToolImage.label != null)
                        imageObj.put("label", digiToolImage.label);                    
                                        
                    if(imageObj.get("pid") != null){    
/*                        
                        //check if image already exists, only add if it does not exist
                        Boolean imageExist = this.imageExists(imageObj.get("pid").toString());
                        if(imageExist == null){
                            this.requestLog.log(Level.SEVERE, "Error while image exists check for image; {}. Skip this image", imageObj.get("pid").toString());
                            continue;
                        }
                        if(imageExist == true){
                            this.requestLog.log(Level.SEVERE, "Image {0} already exists therefor skip", imageObj.get("pid").toString());
                            continue;                            
                        }
*/                            
                        // Image(pid) will be added for this item
                        HttpResponse response = this.addResource("digi_tool", null, true, null, imageObj.toString(), "application/json");                        
                        if(response != null){
                            HttpEntity entityResponse= response.getEntity();
                            String responseString = EntityUtils.toString(entityResponse, "UTF-8");
                            this.requestLog.log(Level.INFO, "Add image response: {0}", responseString);                                    

                            if(response.getStatusLine().getStatusCode() != 201){
                                this.requestLog.log(Level.SEVERE, "--Add image faild (Message): {0}", responseString);
                                this.requestLog.log(Level.SEVERE, "--Add image faild(Code): {0}", response.getStatusLine().getStatusCode());
                                System.out.println("---->Add image faild: " + responseString);
                            }
                            else{
                                this.requestLog.log(Level.SEVERE, "Image addedd successfully: {0}", responseString);                        
                                System.out.println("---->Image ("+ imageObj.get("pid") +" ) added successfully: ");
                            }                                
                        }
                        else
                            this.requestLog.log(Level.INFO, "Add image faild. Error in POST request to omeka digitoolurl");                         
                                                
                    }
                    this.requestLog.log(Level.SEVERE, "Adding image failed. Pid missing");
                }
                
            }
            else
                this.requestLog.log(Level.SEVERE, "Adding image failed. Item id missing.");            
            
        } catch (ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Adding image exception: {0}", ex.getMessage());
            this.digitoolimages.clear();
        } catch (IOException | org.apache.http.ParseException ex) {
            this.requestLog.log(Level.SEVERE, "Adding image IO exception: {0}", ex.getMessage());
            this.digitoolimages.clear();
        }
        
        this.digitoolimages.clear();
    }
    
    public void setDigiToolPid(String pidString){                
        if(pidString.length() > 0){            
            DigiTool image = new DigiTool();
            image.pid = pidString;
            this.digitoolimages.add(image);
            image = null;
                    
        }
        else
            this.requestLog.log(Level.SEVERE, "Invalid digitool pid given: {0}", pidString);
    }
    
    public Boolean imageExists(String pid){
//        try {
//            URI uri = this.prepareRequst("digi_tool", pid, false, null);
//            HttpGet httpget = new HttpGet(uri);
//            HttpClient httpclient = new DefaultHttpClient();
//            HttpResponse response = httpclient.execute(httpget);
//            HttpEntity entity = response.getEntity();
//            
//            JSONParser parser = new JSONParser();
//            JSONObject messageBody = (JSONObject) parser.parse(EntityUtils.toString(entity, "UTF-8"));
//            System.out.println(messageBody);
//            
//
//
//        } catch (ParseException | IOException ex) {
//            this.requestLog.log(Level.SEVERE, "Pid exist check exception: {0}", ex.getMessage());
//            return null;
//        } 
        return true;
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
                /* Different base url for browse */
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
                
            case "geo_location":
                path = "/"+ this.omekaServerConfig.getProperty("url_geo_location");              
                break;     
                
            case "digi_tool":
                path = "/"+ this.omekaServerConfig.getProperty("url_digi_tool");              
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
            this.requestLog.log(Level.SEVERE, "Prepare url Exception: {0}", ex.getMessage());
        }
        
        this.requestLog.log(Level.INFO, "Prepare url: {0}", uri);
        return uri;
    }
        
    
}
