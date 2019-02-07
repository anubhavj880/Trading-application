package co.biz.cexio.util;

import java.io.IOException;
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
public class CexioSingletonClass {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(CexioSingletonClass.class);
	private static CexioSingletonClass adlConnectionObj;
	private static final String CLIENTID = "f26b6f5e-c581-41ba-a835-96d3325a4ab2";
	private static final String AUTHTOKENENDPOINT = "https://login.windows.net/cac8ed95-253c-4ea2-9f59-0f809f87957a/oauth2/token";
	private static final String CLIETNKEY = "bFn1nVqdaVnuTtSbHwkeh5vzXzWMTrTEC6+3Lfb5ROQ=";
	private static final String ACCOUNTFQDN = "pare.azuredatalakestore.net";
	private static AccessTokenProvider provider = new ClientCredsTokenProvider(AUTHTOKENENDPOINT, CLIENTID, CLIETNKEY);
	private static ADLStoreClient client = ADLStoreClient.createClient(ACCOUNTFQDN, provider);

	private CexioSingletonClass() {

	}

	public static CexioSingletonClass getInstance() throws IOException {
		if (adlConnectionObj == null) {
			adlConnectionObj = new CexioSingletonClass();
		}
		return adlConnectionObj;
	}

	public ADLStoreClient getADLStoreClient() {
		return client;
	}
}
