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
package vn.ds.study;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPBody;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.wso2.carbon.connector.core.ConnectException;
import vn.ds.study.utils.OMElementUtils;

import java.util.Base64;

@Slf4j
public class GetObject extends MinioAgent {

    public static byte[] downloadObjectIntoMemory(
            String projectId, String bucket, String objectKey) {

        Storage storage = StorageOptions.newBuilder()
                                        .setProjectId(projectId)
                                        .build()
                                        .getService();
        return storage.readAllBytes(bucket, objectKey);
    }

    @Override
    protected void execute(final MessageContext messageContext) throws ConnectException {

        Axis2MessageContext context = (Axis2MessageContext) messageContext;

        final String projectId = getParameterAsString("projectId");
        final String bucket = getParameterAsString("bucket");
        final String objectKey = getParameterAsString("objectKey");
        try {
            final String googleApplicationCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (googleApplicationCredentials == null || googleApplicationCredentials.isEmpty()) {
                log.error("Missing GOOGLE_APPLICATION_CREDENTIALS environment variable! GCS client use this to locate the credential file");
                context.setProperty("putObjectResult", false);
                return;
            }
            log.info("Get object {} from GCS", objectKey);

            final byte[] output = downloadObjectIntoMemory(projectId, bucket, objectKey);
            if (output == null || output.length <= 0) {
                return;
            }
            final String base64Output = Base64.getEncoder()
                                              .encodeToString(output);

            final OMElement resultElement = OMElementUtils.createOMElement("getObjectResult", true);
            final OMElement binaryElement = OMElementUtils.createOMElement("binaryObject", base64Output);


            final SOAPBody soapBody = messageContext.getEnvelope()
                                                    .getBody();
            JsonUtil.removeJsonPayload(context.getAxis2MessageContext());
            context.getAxis2MessageContext()
                   .removeProperty(
                           PassThroughConstants.NO_ENTITY_BODY);

            soapBody.addChild(resultElement);
            soapBody.addChild(binaryElement);

            log.info("Complete getting object {} from GCS", objectKey);
        } catch (Exception e) {
            log.error("", e);
            throw new ConnectException(e, "Failed to download file. Detail: ");
        }
    }
}