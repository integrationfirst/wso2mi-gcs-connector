package vn.ds.study.mi.connector.gcs;

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;

import java.util.Optional;

public abstract class MinioAgent extends AbstractConnector {

    private MessageContext context;

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

    protected String getParameterAsString(String parameterName, String defaultValue) {
        return Optional.ofNullable(getParameterAsString(parameterName))
                       .orElse(defaultValue);

    }

    protected abstract void execute(final MessageContext messageContext) throws ConnectException;

}
