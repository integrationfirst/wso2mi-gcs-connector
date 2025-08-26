/*
 * Class: GetObject
 *
 * Created on Jan 26, 2022
 *
 * (c) Copyright Swiss Post Solutions Ltd, unpublished work
 * All use, disclosure, and/or reproduction of this material is prohibited
 * unless authorized in writing.  All Rights Reserved.
 * Rights in this program belong to:
 * Swiss Post Solution.
 * Floor 4-5-8, ICT Tower, Quang Trung Software City
 */
package vn.ds.study.mi.connector.gcs.operation;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.transport.TransportUtils;
import org.apache.synapse.MessageContext;
import org.apache.synapse.transport.passthru.util.BinaryRelayBuilder;
import org.wso2.carbon.connector.core.ConnectException;
import vn.ds.study.mi.connector.gcs.MinioAgent;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class GetObject extends MinioAgent {

    public static Blob downloadObjectIntoMemory(
            String projectId,
            String bucket,
            String objectKey) {

        Storage storage = StorageOptions.newBuilder()
                                        .setProjectId(projectId)
                                        .build()
                                        .getService();
        return storage.get(bucket, objectKey, Storage.BlobGetOption.fields(Storage.BlobField.values()));
    }

    @Override
    protected void execute(final MessageContext messageContext) throws ConnectException {

        final String projectId = getParameterAsString("projectId");
        final String bucket = getParameterAsString("bucket");
        final String objectKey = getParameterAsString("objectKey");
        String contentType = getParameterAsString("contentType");

        try {
            final String googleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (googleApplicationCredentials == null || googleApplicationCredentials.isEmpty()) {
                log.error("Missing GOOGLE_APPLICATION_CREDENTIALS environment variable! GCS client use this to locate the credential file");
                return;
            }
            log.info("Get object {} from GCS", objectKey);
            ByteArrayOutputStream os = new ByteArrayOutputStream();

            Blob blob = downloadObjectIntoMemory(projectId, bucket, objectKey);

            contentType = Optional.ofNullable(blob.getContentType())
                                  .orElse(contentType);
            final Map<String, Object> object = new HashMap<>();
            String contentString = null;
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                blob.downloadTo(outputStream);
                byte[] content = outputStream.toByteArray();
                if (contentType.contains("octet-stream")
                        || contentType.contains("image")
                        || contentType.contains("pdf")) {
                    contentString = Base64.getEncoder()
                                          .encodeToString(content);
                } else {
                    contentString = new String(content, StandardCharsets.UTF_8);
                }
            }
            messageContext.setProperty("gcsObjectContentType", contentType);
            messageContext.setProperty("gcsObjectSize", blob.getSize()
                                                            .longValue());
            messageContext.setProperty("gcsObjectContent", contentString);

            log.info("Complete getting object {} from GCS", objectKey);
        } catch (Exception e) {
            log.error("Failed to download object {} from GCS", objectKey, e);
            throw new ConnectException(e);
        }
    }


    /**
     * Build synapse message using inputStream. This will read the stream
     * completely and build the complete message.
     */
    private void buildSynapseMessage(InputStream inputStream,
                                     String contentPropertyName,
                                     MessageContext msgCtx,
                                     String contentType) throws Exception {

        try {
            org.apache.axis2.context.MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.
                    Axis2MessageContext) msgCtx).getAxis2MessageContext();
            Builder builder = selectSynapseMessageBuilder(msgCtx, contentType);
            OMElement documentElement = builder.processDocument(inputStream, contentType, axis2MsgCtx);
            //We need this to build the complete message before closing the stream
//            documentElement.toString();
            if (org.apache.commons.lang.StringUtils.isNotEmpty(contentPropertyName)) {
                msgCtx.setProperty(contentPropertyName, documentElement);
            } else {
                msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
            }
        } catch (Exception e) {
            log.error("Failed to build synapse message", e);
            throw new Exception("Error while building message from Stream", e);
        }
    }

    private Builder selectSynapseMessageBuilder(MessageContext msgCtx, String contentType) throws AxisFault {
        org.apache.axis2.context.MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.
                Axis2MessageContext) msgCtx).getAxis2MessageContext();

        Builder builder;
        if (org.apache.commons.lang.StringUtils.isEmpty(contentType)) {
            log.debug("No content type specified. Using RELAY builder.");
            builder = new BinaryRelayBuilder();
        } else {
            int index = contentType.indexOf(';');
            String type = index > 0 ? contentType.substring(0, index) : contentType;
            builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
            if (builder == null) {
                if (log.isDebugEnabled()) {
                    log.debug("No message builder found for type '" + type + "'. Falling back "
                                      + "to RELAY builder.");
                }
                builder = new BinaryRelayBuilder();
            }
        }
        log.info("Message builder {}", builder != null ? builder.getClass() : "No Builder");
        return builder;
    }
}