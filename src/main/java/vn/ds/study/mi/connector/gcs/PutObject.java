/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package vn.ds.study.mi.connector.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axis2.format.BinaryFormatter;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.base.BaseTransportException;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.lang3.StringUtils;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.relay.ExpandingMessageFormatter;

import javax.activation.DataHandler;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class PutObject extends MinioAgent {

    private static String uploadObjectFromMemory(
            String projectId, String bucketName, String objectKey, InputStream inputStream) {

        try {
            Storage storage = StorageOptions.newBuilder()
                                            .setProjectId(projectId)
                                            .build()
                                            .getService();
            BlobId blobId = BlobId.of(bucketName, objectKey);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                                        .build();
            storage.createFrom(blobInfo, inputStream);
            return blobId.toString();
        } catch (IOException e) {
            log.error("Failed to upload object {} to GCS", objectKey, e);
        }
        return null;
    }

    @Override
    public void execute(MessageContext messageContext) throws ConnectException {

        messageContext.getContextEntries()
                      .keySet()
                      .stream()
                      .map(k -> String.format("KEY: %s", k))
                      .forEach(log::debug);
        Axis2MessageContext context = (Axis2MessageContext) messageContext;

        final Param param = readAndValidateInputs();

        try {
            final String googleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (googleApplicationCredentials == null || googleApplicationCredentials.isEmpty()) {
                log.error("Missing GOOGLE_APPLICATION_CREDENTIALS environment variable! GCS client use this to locate the credential file");
                context.setProperty("putObjectResult", false);
                return;
            }
            log.info("Put object {} to GCS address", param.objectKey);

            boolean result = writeObject(context, param);
            context.setProperty("putObjectResult", result);

            log.info("Complete process to put object {} to OS", param.objectKey);
        } catch (Exception e) {
            log.error("Failed to upload object {} to GCS", param.objectKey, e);
            throw e;
        }
    }

    private boolean writeObject(final Axis2MessageContext context, final Param param) {

        InputStream is = null;

        if (StringUtils.isNotEmpty(param.content)) {
            log.info("Extract object content from property [content]");
            if (Constants.ContentTypes.APPLICATION_BINARY.equalsIgnoreCase(param.contentType)) {
                is = Optional.ofNullable(param.content)
                             .map(Base64.getDecoder()::decode)
                             .map(ByteArrayInputStream::new)
                             .get();
            } else {
                is = Optional.ofNullable(param.content)
                             .map(String::getBytes)
                             .map(ByteArrayInputStream::new)
                             .get();
            }
        } else {
            log.info("Extract object content from body");
            is = Optional.ofNullable(readBodyContent(context, param))
                         .map(ByteArrayInputStream::new)
                         .get();
        }
        if (is == null) {
            log.error("Failed to get input stream for the object {}", param.objectKey);
            return false;
        }

        uploadObjectFromMemory(param.projectId, param.bucket, param.objectKey, is);

        return true;
    }

    private InputStream inputStream(DataHandler dataHandler) {
        try {
            return dataHandler.getInputStream();
        } catch (IOException e) {
            log.error("Failed to obtain input stream from Envelope element", e);
        }

        return null;
    }

    private InputStream inputStream(String text) {
        log.debug("Convert text [{}] into input stream", text);
        return new ByteArrayInputStream(text.getBytes());
    }

    private Param readAndValidateInputs() {
        Param config = new Param();

        config.projectId = getParameterAsString("projectId");
        config.bucket = getParameterAsString("bucket");
        config.objectKey = getParameterAsString("objectKey");
        config.content = getParameterAsString("content");
        config.contentType = getParameterAsString("contentType", Constants.ContentTypes.APPLICATION_BINARY);

        return config;

    }

    private byte[] readBodyContent(MessageContext messageContext, Param config) {

        org.apache.axis2.context.MessageContext axis2MessageContext = ((Axis2MessageContext) messageContext).
                getAxis2MessageContext();

        try {
            if (StringUtils.isNotEmpty(config.contentType) &&
                    !(config.contentType.equals(Constants.ContentTypes.AUTOMATIC))) {

                axis2MessageContext.setProperty(Constants.Props.MESSAGE_TYPE, config.contentType);
            }

            MessageFormatter messageFormatter;
            if (config.enableStreaming) {
                //this will get data handler and access input stream set
                messageFormatter = new ExpandingMessageFormatter();
            } else {
                messageFormatter = getMessageFormatter(axis2MessageContext);
            }

            OMOutputFormat format = BaseUtils.getOMOutputFormat(axis2MessageContext);
            if (Objects.isNull(messageFormatter)) {
                log.error("Could not determine the message formatter for {}", config.objectKey);
                return null;
            }
            log.debug("Formatter {}", messageFormatter.getClass());
            final byte[] bytes = messageFormatter.getBytes(axis2MessageContext, format);
            return Optional.of(bytes)
                           .get();

        } catch (Exception e) {
            log.error("Failed to read message body {}", config.objectKey, e);
        }

        return null;
    }

    private MessageFormatter getMessageFormatter(org.apache.axis2.context.MessageContext msgContext) {
        OMElement firstChild = msgContext.getEnvelope()
                                         .getBody()
                                         .getFirstElement();
        if (firstChild != null) {
            if (BaseConstants.DEFAULT_BINARY_WRAPPER.equals(firstChild.getQName())) {
                return new BinaryFormatter();
            } else if (BaseConstants.DEFAULT_TEXT_WRAPPER.equals(firstChild.getQName())) {
                return new PlainTextFormatter();
            }
        }
        try {
            return MessageProcessorSelector.getMessageFormatter(msgContext);
        } catch (Exception e) {
            throw new BaseTransportException("Unable to get the message formatter to use");
        }
    }


    private class Param {
        public boolean enableStreaming;
        String projectId;
        String bucket;
        String objectKey;
        String content;
        String contentType;
    }

}