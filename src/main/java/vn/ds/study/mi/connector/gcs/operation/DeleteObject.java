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

import com.google.cloud.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.core.ConnectException;
import vn.ds.study.mi.connector.gcs.MinioAgent;
import java.util.Optional;

@Slf4j
public class DeleteObject extends MinioAgent {

    private static boolean deleteObject(
            String projectId, String bucketName, String objectKey) {

        try {
            Storage storage = StorageOptions.newBuilder()
                                            .setProjectId(projectId)
                                            .build()
                                            .getService();
            BlobId blobId = BlobId.of(bucketName, objectKey);

            storage.delete(blobId);
            log.info("Delete object {} successfully", objectKey);
            return true;
        } catch (Exception e) {
            log.error("Failed to delete object {} from GCS", objectKey, e);
        }
        return false;
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

        try {
            final String googleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (googleApplicationCredentials == null || googleApplicationCredentials.isEmpty()) {
                log.error("Missing GOOGLE_APPLICATION_CREDENTIALS environment variable! GCS client use this to locate the credential file");
                context.setProperty("deleteObjectResult", false);
                return;
            }

            log.info("Delete object {} from GCS", objectKey);
            final boolean deleteResult = Optional.of(objectKey)
                                                 .map(i -> deleteObject(projectId, bucket, i))
                                                 .orElse(false);
            context.setProperty("deleteObjectResult", deleteResult);

            log.info("Complete process to delete object {} from OS", objectKey);
        } catch (Exception e) {
            log.error("Failed to delete object {} from GCS", objectKey, e);
            throw e;
        }
    }

}