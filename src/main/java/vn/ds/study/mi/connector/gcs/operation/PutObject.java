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
package vn.ds.study.mi.connector.gcs.operation;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.core.ConnectException;
import vn.ds.study.mi.connector.gcs.MinioAgent;

import javax.activation.DataHandler;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Optional;

@Slf4j
public class PutObject extends MinioAgent {

    private static String uploadObjectFromMemory(
            String projectId,
            String bucketName,
            String objectKey,
            String contentType,
            InputStream inputStream) {

        try {
            Storage storage = StorageOptions.newBuilder()
                                            .setProjectId(projectId)
                                            .build()
                                            .getService();
            BlobId blobId = BlobId.of(bucketName, objectKey);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                                        .setContentType(contentType)
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
                      .forEach(log::info);
        Axis2MessageContext context = (Axis2MessageContext) messageContext;

        String projectId = getParameterAsString("projectId");
        String bucket = getParameterAsString("bucket");
        String objectKey = getParameterAsString("objectKey");
        String contentType = Optional.of(getParameterAsString("contentType"))
                                     .orElse("application/json");
        Object rawContent = getParameter(context, "content");

        try {
            final String googleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (googleApplicationCredentials == null || googleApplicationCredentials.isEmpty()) {
                log.error("Missing GOOGLE_APPLICATION_CREDENTIALS environment variable! GCS client use this to locate the credential file");
                context.setProperty("putObjectResult", false);
                return;
            }

            InputStream is = null;
            log.info("Start putting object {} and content type: {}", objectKey, contentType);

            if (contentType.contains("json")
                    || contentType.contains("text")
                    || contentType.contains("xml")
                    || contentType.contains("html")
                    || contentType.contains("javascript")
                    || contentType.contains("jwt")) {
                log.info("Text-based content");
                is = new ByteArrayInputStream(String.valueOf(rawContent)
                                                    .getBytes());
            } else {
                log.info("Binary content {}", rawContent.getClass());
                is = new ByteArrayInputStream(Base64.getDecoder()
                                                    .decode((String) rawContent));
            }

            log.info("Put object {} to GCS address", objectKey);
            final Boolean uploadResult = Optional.of(is)
                                                 .map(i -> uploadObjectFromMemory(projectId, bucket, objectKey, contentType, i))
                                                 .map(res -> res != null && !res.isEmpty())
                                                 .orElse(false);
            context.setProperty("putObjectResult", uploadResult.booleanValue());

            log.info("Complete process to put object {} to OS", objectKey);
        } catch (Exception e) {
            log.error("Failed to upload object {} to GCS", objectKey, e);
            throw e;
        }
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

}