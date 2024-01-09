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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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

    private static String uploadObjectFromMemory(
            String projectId, String bucketName, String objectKey, InputStream inputStream) {

        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of your GCS bucket
        // String bucketName = "your-unique-bucket-name";

        // The ID of your GCS object
        // String objectName = "your-object-name";

        // The string of contents you wish to upload
        // String contents = "Hello world!";

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

        Axis2MessageContext context = (Axis2MessageContext) messageContext;

        String projectId = getParameterAsString("projectId");
        String bucket = getParameterAsString("bucket");
        String objectKey = getParameterAsString("objectKey");
        String accessKey = getParameterAsString("accessKey");
        String secretKey = getParameterAsString("secretKey");

        final String googleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (googleApplicationCredentials == null || googleApplicationCredentials.isEmpty()) {
            log.error("Missing GOOGLE_APPLICATION_CREDENTIALS environment variable! GCS client use this to locate the credential file");
            context.setProperty("putObjectResult", false);
            return;
        }

        log.info("Put object {} to GCS address", objectKey);

        final Boolean uploadResult = Optional.of(context)
                                             .map(Axis2MessageContext::getEnvelope)
                                             .map(SOAPEnvelope::getBody)
                                             .map(OMElement::getFirstElement)
                                             .map(OMContainer::getFirstOMChild)
                                             .map(OMText.class::cast)
                                             .map(OMText::getDataHandler)
                                             .map(DataHandler.class::cast)
                                             .map(this::inputStream)
                                             .map(is -> uploadObjectFromMemory(projectId, bucket, objectKey, is))
                                             .map(res -> res != null && !res.isEmpty())
                                             .orElse(false);
        context.setProperty("putObjectResult", uploadResult.booleanValue());

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

}