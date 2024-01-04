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
package vn.ds.study;

import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import lombok.extern.slf4j.Slf4j;
import org.apache.axiom.om.OMContainer;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMText;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.core.ConnectException;

import javax.activation.DataHandler;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;


@Slf4j
public class PutObject extends MinioAgent {

    @Override
    public void execute(MessageContext messageContext) throws ConnectException {

        Axis2MessageContext context = (Axis2MessageContext) messageContext;

        String address = getParameterAsString("address");
        String bucket = getParameterAsString("bucket");
        String objectKey = getParameterAsString("objectKey");
        String accessKey = getParameterAsString("accessKey");
        String secretKey = getParameterAsString("secretKey");

        log.info("Put object {} to OS address {}", objectKey, address);
        MinioClient client = getClient(address, accessKey, secretKey);

        if (client == null) {
            log.info("Failed to login into OS with access: {} and secret: {}", accessKey, secretKey);
            return;
        }

        log.info("Successfully login into OS");

        Optional.of(context)
                .map(Axis2MessageContext::getEnvelope)
                .map(SOAPEnvelope::getBody)
                .map(OMElement::getFirstElement)
                .map(OMContainer::getFirstOMChild)
                .map(OMText.class::cast)
                .map(OMText::getDataHandler)
                .map(DataHandler.class::cast)
                .map(this::inputStream)
                .map(is -> putObject(is, client, bucket, objectKey))
                .map(res -> {
                    Optional.ofNullable(res)
                            .map(ObjectWriteResponse::object)
                            .map(o -> "Processed object " + o)
                            .ifPresent(log::info);
                    
                    messageContext.setProperty("putObjectResult", "SUCCESS");
                    log.info("Put object {} to OS successfully", objectKey);
                    return res;
                });
        log.info("Complete process to put object {} to OS", objectKey);
    }

    private InputStream inputStream(DataHandler dataHandler) {
        try {
            return dataHandler.getInputStream();
        } catch (IOException e) {
            log.error("Failed to obtain input stream from Envelope element", e);
        }

        return null;
    }

    private ObjectWriteResponse putObject(final InputStream is, final MinioClient client, final String bucket,
                                          final String objectKey) {

        try (is) {
            log.info("Putting object {}", objectKey);
            return client.putObject(
                    PutObjectArgs.builder()
                                 .bucket(bucket)
                                 .object(objectKey)
                                 .stream(is, -1, 10485760)
                                 .build());
        } catch (Exception e) {
            log.error("Failed to execute putObject", e);
        }

        return null;
    }
}