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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.core.ConnectException;
import vn.ds.study.mi.connector.gcs.MinioAgent;
import vn.ds.study.mi.connector.gcs.model.ListObjectResult;
import vn.ds.study.mi.connector.gcs.model.ObjectInfo;
import vn.ds.study.mi.connector.gcs.utils.OMElementUtils;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ListObjectByPrefix extends MinioAgent {

    public static final String OBJECT_KEYS = "objs";
    private static final String OBJECT_KEY = "obj";

    @Override
    protected void execute(final MessageContext messageContext) throws ConnectException {

        Axis2MessageContext context = (Axis2MessageContext) messageContext;

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

            List<ObjectInfo> objs = listObjectByKeyPrefix(projectId, bucket, objectKeyPrefix);
            OMElement filesEle = OMElementUtils.createOMElement(OBJECT_KEYS, null);
            objs.forEach(k->{
                OMElement objKey = OMElementUtils.createOMElement(OBJECT_KEY, null);
                OMElement keyElement = OMElementUtils.createOMElement("key", k.getKey());
                objKey.addChild(keyElement);
                filesEle.addChild(objKey);
            });
            final ListObjectResult result = ListObjectResult.builder()
                                                            .operation("listObjectByPrefix")
                                                            .isSuccessful(true)
                                                            .resultEle(filesEle)
                                                            .build();
            OMElementUtils.setResultAsPayload(messageContext, result);
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
        for (Blob blob : blobs.iterateAll()) {
            blobs.getValues().forEach(obj->{
                res.add(ObjectInfo.builder().key(obj.getName()).build());
            });
        }

        return res;
    }
}