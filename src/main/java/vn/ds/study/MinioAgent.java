package vn.ds.study;

import io.minio.MinioClient;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;

public abstract class MinioAgent extends AbstractConnector {

    private MessageContext context;

    protected MinioClient getClient(String address, final String accessKey, final String secretKey) {
        return MinioClient.builder()
                          .endpoint(address)
                          .credentials(accessKey, secretKey)
                          .build();
    }

    @Override
    public final void connect(final MessageContext messageContext) throws ConnectException {
        this.context = messageContext;
        execute(messageContext);
    }

    @SuppressWarnings("unchecked")
    protected <T> T getParameter(String parameterName, Class<T> type) {
        return (T) getParameter(context, parameterName);
    }

    protected String getParameterAsString(String parameterName) {
        return getParameter(parameterName, String.class);
    }

    protected abstract void execute(final MessageContext messageContext) throws ConnectException;

}
