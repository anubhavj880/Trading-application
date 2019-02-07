package co.biz.exmo.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

/**
 * 
 * @author Dhinesh Raja
 *
 */
public class ExmoSingletonClass {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(ExmoSingletonClass.class);
	private static ExmoSingletonClass adlConnectionObj;
	private static Properties adlProp = new Properties();
	static{
		
    	InputStream adlPropStream = null;
    	try {
    		adlPropStream = ExmoSingletonClass.class.getClassLoader().getResourceAsStream("datalake.properties");
    		
    		if(adlPropStream==null){
    	            slf4jLogger.error("Sorry, unable to find " + "datalake.properties");
    		    
    		}
    		
    		adlProp.load(adlPropStream);
    		
    		
    	} catch (IOException ex) {
    		 slf4jLogger.error(ex.getMessage());
    	}
    		 catch (Exception allEx) {
        		 slf4jLogger.error(allEx.getMessage());
        } finally{
        	if(adlPropStream!=null){
        		try {
        			adlPropStream.close();
			} catch (IOException e) {
				slf4jLogger.error(e.getMessage());
			}
        	}
        	
        }
	}
	private static final String CLIENTID = adlProp.getProperty("clientid");
	private static final String AUTHTOKENENDPOINT = adlProp.getProperty("endpoint");
	private static final String CLIETNKEY = adlProp.getProperty("key");
	private static final String ACCOUNTFQDN = adlProp.getProperty("account");
	private static AccessTokenProvider provider = new ClientCredsTokenProvider(AUTHTOKENENDPOINT, CLIENTID, CLIETNKEY);
	private static ADLStoreClient client = ADLStoreClient.createClient(ACCOUNTFQDN, provider);

	private ExmoSingletonClass() {

	}

	public static ExmoSingletonClass getInstance() throws IOException {
		if (adlConnectionObj == null) {
			adlConnectionObj = new ExmoSingletonClass();
		}
		return adlConnectionObj;
	}

	public ADLStoreClient getADLStoreClient() {
		return client;
	}
}
