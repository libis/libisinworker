package libisinworker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject; 
import org.json.simple.parser.JSONParser; 
import org.json.simple.parser.ParseException;

/**
 *
 * @author NaeemM
 */
public class Libisinworker {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ParseException, URISyntaxException {    
              
        LibisinUtil libisinUtils = new LibisinUtil();       
        Logger serverLog = libisinUtils.createLogger(null, Libisinworker.class.getName());
                        
        Libisinworker worker = new Libisinworker();
        Properties queuingServerConfig = worker.getConfigurations("queuingserver", serverLog);
        Properties caServerConfig = worker.getConfigurations("caserver", serverLog);
        Properties dmtServiceConfig = worker.getConfigurations("dmtservice", serverLog);
        Properties omekaServiceConfig = worker.getConfigurations("omekaserver", serverLog);
        Properties libisinWorkerConfig = worker.getConfigurations("libisinworker", serverLog);
       
        Connection connection = worker.connectQueuingServer(queuingServerConfig, serverLog);
        Channel channel = connection.createChannel();        
        channel.queueDeclare(queuingServerConfig.getProperty("rmq_queue_name").trim(), false, false, false, null);
                
        QueueingConsumer consumer = new QueueingConsumer(channel);        
        channel.basicConsume(queuingServerConfig.getProperty("rmq_queue_name").trim(), true, consumer);    
                 
        String connectionMessage = "Connection established to: \n" 
            + "---------  \n"
            + "server: " + queuingServerConfig.getProperty("rmq_server") + " \n"
            + "port: " + queuingServerConfig.getProperty("rmq_port") + " \n"
            + "Queue: " + queuingServerConfig.getProperty("rmq_queue_name").trim() + " \n"
            + "Consumer tag: " + consumer.getConsumerTag() + " \n"
            + "Startup time: " + (new Date()) + " \n"
            + "user: " + queuingServerConfig.getProperty("rmq_id") + " \n"
            + "---------  \n";
        
        System.out.println(connectionMessage);        
        serverLog.log(Level.INFO, connectionMessage);
                
        String omekaBaseUrl = omekaServiceConfig.getProperty("omeka_url_base");
        String omeka = omekaBaseUrl.substring(0,omekaBaseUrl.lastIndexOf("/index.php"));
        serverLog.log(Level.INFO, "Omeka server: http://{0}", omeka);

        String dmtBaseUrl = dmtServiceConfig.getProperty("dmt_url_base");
        String mappingService = dmtBaseUrl.substring(0,dmtBaseUrl.lastIndexOf("/dmt.php"));
        serverLog.log(Level.INFO, "DMT Service: http://{0}", mappingService);   
        
        String collectiveAccessBaseUrl = caServerConfig.getProperty("ca_server") 
                +"/"+ caServerConfig.getProperty("ca_base_path");
        String collectiveAccess = collectiveAccessBaseUrl.substring(0,collectiveAccessBaseUrl.lastIndexOf("/service.php"));
        serverLog.log(Level.INFO, "Collective Access server: http://{0}", collectiveAccess);         
        
        System.out.println("---------");
        System.out.println("Collective Access:  " + collectiveAccess);
        System.out.println("Mapping Service:    " + mappingService );
        System.out.println("Omeka:              " + omeka);
        System.out.println("---------");
        
        System.out.println("[*] Waiting for messages. To exit press CTRL+C"); 
                
        JSONParser parser = new JSONParser();
        Cadata setData = new Cadata();
        DmtService dmtService = new DmtService();
        OmekaData omekaRecords = new OmekaData(omekaServiceConfig);      
                        
        while (true) {            
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            
            String message = new String(delivery.getBody());   
            JSONObject messageBodyobj = (JSONObject) parser.parse(message); 
            
            // Worker can handel request messages and command messages
            // Stop worker gracefully against command message
            // Command message structure should be like {"name":"libisinworker","command":"stop"}
            if(messageBodyobj.containsKey("command".toLowerCase()) &&
                    messageBodyobj.containsKey("name".toLowerCase())){

                /* Check if request is received for right worker. */
                if(!messageBodyobj.get("name".toLowerCase()).toString().equals("libisinworker"))
                    continue;
                    
                if(messageBodyobj.get("command".toLowerCase()).toString().equals("stop")){
                    System.out.println(messageBodyobj);
                    System.out.println("Received 'Stop' command.");    
                    serverLog.log(Level.INFO, "Command received: {0}", messageBodyobj);
                    String queueDeleted = channel.queueDelete(queuingServerConfig.getProperty("rmq_queue_name")).toString();
                    connection.close();
                    
                    System.out.println(queueDeleted);
                    serverLog.log(Level.INFO, "Delete queue {0}.", queueDeleted);                                        
                    serverLog.log(Level.INFO, "Connection closed.");
                    System.exit(-1);
                }
            }
            
            String requestDirectory = libisinUtils.createRequestDirectory();
            if(requestDirectory == null){
                System.out.println("Error in creating request directory. Stopping further processing of this request.");
                serverLog.log(Level.INFO, "Error in creating request directory: {0}, request will not be processed", requestDirectory);
                continue;
            }            
            
            String reportFile = requestDirectory+ "/report.txt";
            String requestLogFile = requestDirectory + "/request_log.txt";
            Logger requestLog = libisinUtils.createLogger(requestLogFile, setData.getClass().getName());
            omekaRecords.requestLog = requestLog;
            dmtService.requestLog = requestLog;
            setData.requestLog = requestLog;
            
            //Prepare list of omeka elements, this list will be used for each record.
            System.out.println("--->Collecting omeka elements information.");
            serverLog.log(Level.INFO, "Collecting omeka elements information.");
            omekaRecords.omekaElements = omekaRecords.getTypesElements();             
            
            System.out.println("+---------------------------");
            Timestamp requestT1 = new Timestamp(new java.util.Date().getTime());
            System.out.println("Request processing started at: " + requestT1);
            System.out.println("Request directory: " + new File(requestDirectory).getName());
            serverLog.log(Level.INFO, "Request processing started at: {0}", new Timestamp(new java.util.Date().getTime()));
            serverLog.log(Level.INFO, "Request directory created: {0}", requestDirectory);
                        
            JSONObject userObj = (JSONObject) parser.parse(messageBodyobj.get("user_info").toString());
            String setInfoBody = messageBodyobj.get("set_info").toString();
            
            serverLog.log(Level.INFO, "Request info: {0}", setInfoBody);
            serverLog.log(Level.INFO, "User info: {0}", userObj);
            
            System.out.println("Processing Request of user: "+ userObj.get("name").toString());
            JSONArray setInfoBodyArray = (JSONArray) messageBodyobj.get("set_info");
            for(int i=0; i< setInfoBodyArray.size(); i++){
                                
                JSONObject object = (JSONObject)setInfoBodyArray.get(i);                                                
                String mappingRules = object.get("mapping").toString();                              
				String mappingFilePath = libisinUtils.writeFile(requestDirectory + "/mappingrules_"+object.get("set_name").toString()+".csv", mappingRules, false);				
                serverLog.log(Level.INFO, "Mapping file: {0}", mappingFilePath);
                                
                ////retrieve records from collectiveaccess
                String caRecordsFilePath = setData.getSetData(object.get("set_name").toString(),
                        caServerConfig, object.get("bundle").toString(), object.get("record_type").toString(), requestDirectory); 
                
                if(caRecordsFilePath == null){
                    System.out.println("Error in receiving data from Collective Access.");
                    serverLog.log(Level.SEVERE, "Error in receiving data from Collective Access");
                    requestLog.log(Level.SEVERE, "Error in receiving data from Collective Access");
                    return;
                }
                requestLog.log(Level.INFO, "Collective Access records file: {0}", caRecordsFilePath);

                ////send records to mapping service to map to omeka format               
                String dmtMapResponse = dmtService.mapData(caRecordsFilePath, mappingFilePath, dmtServiceConfig);
                               
                JSONObject requestIdobj = (JSONObject) parser.parse(dmtMapResponse);
                String dmtRequestId = requestIdobj.get("request_id").toString();                
                                                
                requestLog.log(Level.INFO, "DMT mapping request id: {0}", dmtRequestId);
                            
                ////fetch mapping result
                String omekaData = dmtService.fetchOmekaDmt(dmtRequestId, dmtServiceConfig);                                                                                                      
                if(omekaData.length() > 0) {
                    requestLog.log(Level.INFO, "DMT fetch successfull. Length of omeka data to add/update: {0} characters", omekaData.length());
                    
                    ///temp_start
                    // Disabled temporary, untill it is complete
                    /* Prepare a list of elements with their relationship types. */
                    //omekaRecords.normalizeDmtData(object.get("bundle").toString(), setData.getTypes(caServerConfig, requestDirectory));
                    ///temp_end
                    
                    boolean omekaSuccess = omekaRecords.pushDataToOmeka(omekaData, object.get("record_type").toString(), 
                            requestDirectory, object.get("set_name").toString());
                    if(omekaSuccess == true){
                        requestLog.log(Level.INFO, "Records pushed to Omeka successfully");    
                        worker.prepareReport(object.get("set_name").toString(), omekaRecords,reportFile , libisinUtils);
                    }
                    else
                        requestLog.log(Level.INFO, "Records pushing to Omeka failed");    
                }                    
                else{
                    System.out.println("No data to add/update in omeka.");
                    serverLog.log(Level.SEVERE, "no data to add/update in omeka");
                    requestLog.log(Level.SEVERE, "no data to add/update in omeka");
                }                                                                                                                   
            }
            
            Timestamp requestT2 = new Timestamp(new java.util.Date().getTime());
            System.out.println("Request processing finished at: " + requestT2);
            requestLog.log(Level.INFO, "Request processing finished at: {0}", requestT2);
            
            double processingTime = (((requestT2.getTime()) - requestT1.getTime())/(60000.0));
            DecimalFormat df = new DecimalFormat("#0.000");
            System.out.println("Total processing time: " + df.format(processingTime) + " minutes.");
            requestLog.log(Level.INFO, "Total processing time: {0} minutes", df.format(processingTime));
            
            libisinUtils.sendEmail(libisinWorkerConfig, userObj.get("email").toString(), userObj.get("name").toString(), 
                    requestLogFile,  requestDirectory+ "/report.txt", requestLog);
            
            System.out.println("---------------------------+\n");
            libisinUtils.destroyLogger(requestLog);                                          
        }        

    }
            
    public void prepareReport(String setName, OmekaData omekaRecord, String reportFile, LibisinUtil libisinUtils){
        String report = "Set Name: " + setName + ", Total Records: " + omekaRecord.totalRecords + "\n" 
                + "Valid Records: " + omekaRecord.validRecords + ", Invalid Records: "+ omekaRecord.invalidRecords +"\n" 
                + "Added Records: " + omekaRecord.addedRecords + ", Updated Records: "+ omekaRecord.updatedRecords 
                +", Failed Records: " + omekaRecord.failedRecords +"\n"
                + "---------\n";
        libisinUtils.writeFile(reportFile, report, true);        
    }
    
    public Connection connectQueuingServer(Properties queuingServerConfig, Logger serverLog){        
        ConnectionFactory factory = new ConnectionFactory();        
        factory.setHost(queuingServerConfig.getProperty("rmq_server"));
        factory.setPort(Integer.parseInt(queuingServerConfig.getProperty("rmq_port")));
        factory.setUsername(queuingServerConfig.getProperty("rmq_id"));
        factory.setPassword(queuingServerConfig.getProperty("rmq_pwd"));
        factory.setVirtualHost(queuingServerConfig.getProperty("rmq_vhost"));
       
        Connection connection = null;
        try {            
            connection = factory.newConnection();            
        } catch (IOException ex) {
            serverLog.log(Level.SEVERE, "Error in connecting queuing server: {0}", queuingServerConfig.getProperty("rmq_server"));
            serverLog.log(Level.SEVERE, "Exception Message: {0}", ex.getMessage());
            System.out.println("Error in connecting queuing server: " + queuingServerConfig.getProperty("rmq_server"));
            System.exit(-1);
        }
        return connection;
    }
    
    public Properties getConfigurations(String configuratinFor, Logger serverLog){
        Properties prop = new Properties();
        String configFile = "";
        switch(configuratinFor){
            case "queuingserver":
                //configFile = "queue_server_conf";             // local config
                configFile = "remote_queue_server_conf";      // remote sandbox config
                //configFile = "pr_queue_server_conf";            // production
                break;
                
            case "caserver":
                //configFile = "ca_server_conf";
                configFile = "remote_ca_server_conf";
                //configFile = "pr_ca_server_conf";               // production
                break;                
                
            case "dmtservice":
                //configFile = "dmt_service_conf";
                configFile = "remote_dmt_service_conf";
                //configFile = "pr_dmt_service_conf";            // production
                break;                 
                
            case "omekaserver":
                //configFile = "omeka_server_conf";
                configFile = "remote_omeka_server_conf";
                //configFile = "pr_omeka_server_conf";          // production
                break;   

            case "libisinworker":
                configFile = "libisinworker_conf";
                break; 
        }
        
        try {                        
            prop.load(this.getClass().getResourceAsStream("/resources/" + configFile + ".properties"));                       						
            
        } catch (IOException | NullPointerException ex) {            
            serverLog.log(Level.SEVERE, "Error in reading configuration file for: {0}", configuratinFor);
            serverLog.log(Level.SEVERE, "File '{0}' not found", configFile);
            serverLog.log(Level.SEVERE, "Exception Message: {0}", ex.getMessage());
            System.out.println("Configuration file not found. Quitting.");
            System.exit(-1);
        } 		  
        return prop; 		  
    }    
                      
}