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

import java.io.InputStream;
import java.util.Base64;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPBody;
import org.apache.commons.io.IOUtils;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.connector.core.ConnectException;

import io.minio.DownloadObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import junit.framework.Assert;
import vn.ds.study.utils.OMElementUtils;

public class GetObject extends MinioAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetObject.class);

    @Override
    protected void execute(final MessageContext messageContext) throws ConnectException {

        final String address = getParameterAsString("address");
        final String bucket = getParameterAsString("bucket");
        final String filename = getParameterAsString("filename");
        final String accessKey = getParameterAsString("accessKey");
        final String secretKey = getParameterAsString("secretKey");
        try {
            final MinioClient client = getClient(address, accessKey, secretKey);

            LOGGER.debug("Created a minio client. {}", client);

            Assert.assertNotNull("Minio client initialization failed", client);

            final DownloadObjectArgs downloadObjectArgs = DownloadObjectArgs.builder().bucket(bucket).object(
                filename).build();
            LOGGER.info("Prepared download object arguments.");

            final GetObjectArgs args = new GetObjectArgs(downloadObjectArgs);

            LOGGER.debug("Prepared arguments.");
            final GetObjectResponse getObjectResponse = client.getObject(args);

            try (final InputStream in = getObjectResponse;) {
                final byte[] output = IOUtils.toByteArray(in);

                final String base64Output = Base64.getEncoder().encodeToString(output);
                
                final OMElement resultElement = OMElementUtils.createOMElement("objectResult", null);
                final OMElement binaryElement = OMElementUtils.createOMElement("binaryObject", base64Output);

                resultElement.addChild(binaryElement);

                final SOAPBody soapBody = messageContext.getEnvelope().getBody();
                JsonUtil.removeJsonPayload(((Axis2MessageContext) messageContext).getAxis2MessageContext());
                ((Axis2MessageContext) messageContext).getAxis2MessageContext().removeProperty(
                    PassThroughConstants.NO_ENTITY_BODY);

                soapBody.addChild(resultElement);
            }
            LOGGER.info("Get the object and put to the objectResult property.");
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new ConnectException(e, "Failed to download file. Detail: ");
        }
    }
}