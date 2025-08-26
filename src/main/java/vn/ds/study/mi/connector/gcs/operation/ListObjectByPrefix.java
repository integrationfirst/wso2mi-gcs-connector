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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.core.ConnectException;
import vn.ds.study.mi.connector.gcs.MinioAgent;
import vn.ds.study.mi.connector.gcs.model.ObjectInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class ListObjectByPrefix extends MinioAgent {

    @Override
    protected void execute(final MessageContext messageContext) throws ConnectException {

        final String projectId = getParameterAsString("projectId");
        final String bucket = getParameterAsString("bucket");
        final String objectKeyPrefix = getParameterAsString("objectKeyPrefix");

        try {
            final String googleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (googleApplicationCredentials == null || googleApplicationCredentials.isEmpty()) {
                log.error("Missing GOOGLE_APPLICATION_CREDENTIALS environment variable! GCS client use this to locate the credential file");
                return;
            }
            log.info("Get object {} from GCS", objectKeyPrefix);

            final ObjectMapper mapper = new ObjectMapper();
            Optional.of(listObjectByKeyPrefix(projectId, bucket, objectKeyPrefix))
                    .map(mapper::valueToTree)
                    .map(n -> n.toString())
                    .ifPresent(objs -> messageContext.setProperty("gcsObjects", objs));
            log.info("Complete getting object {} from GCS", objectKeyPrefix);
        } catch (Exception e) {
            log.error("Failed to download object {} from GCS", objectKeyPrefix, e);
            throw new ConnectException(e);
        }
    }

    private List<ObjectInfo> listObjectByKeyPrefix(final String projectId, final String bucket,
                                                   final String objectKeyPrefix) {
        Storage storage = StorageOptions.newBuilder()
                                        .setProjectId(projectId)
                                        .build()
                                        .getService();
        final List<ObjectInfo> res = new ArrayList<>();

        Page<Blob> blobs = storage.list(bucket, Storage.BlobListOption.prefix(objectKeyPrefix));
        log.info("BLOBS: {}", blobs.toString());
        for (Blob blob : blobs.iterateAll()) {
            res.add(ObjectInfo.builder()
                              .key(blob.getName())
                              .md5(blob.getMd5())
                              .sizeInBytes(blob.getSize()
                                               .longValue())
                              .build());
            log.info("Object with key {}", blob.getName());
        }

        return res;
    }
}