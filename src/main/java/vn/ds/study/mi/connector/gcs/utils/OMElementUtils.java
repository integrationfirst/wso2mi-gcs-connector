/*
 * Class: OMElementUtils
 *
 * Created on Mar 2, 2022
 *
 * (c) Copyright Swiss Post Solutions Ltd, unpublished work
 * All use, disclosure, and/or reproduction of this material is prohibited
 * unless authorized in writing.  All Rights Reserved.
 * Rights in this program belong to:
 * Swiss Post Solution.
 * Floor 4-5-8, ICT Tower, Quang Trung Software City
 */
package vn.ds.study.mi.connector.gcs.utils;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axiom.soap.SOAPBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import vn.ds.study.mi.connector.gcs.model.ListObjectResult;
import vn.ds.study.mi.connector.gcs.utils.Error;

public final class OMElementUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(OMElementUtils.class);

    private OMElementUtils() {

    }

    public static OMElement createOMElement(final String elementName, final Object value) {
        OMElement resultElement = null;
        try {
            if (value != null) {
                resultElement = AXIOMUtil.stringToOM("<" + elementName + ">" + value + "</" + elementName + ">");
            } else {
                resultElement = AXIOMUtil.stringToOM("<" + elementName + "></" + elementName + ">");
            }
        } catch (XMLStreamException | OMException e) {
            LOGGER.error("Error while generating OMElement from element name" + elementName, e);
        }
        return resultElement;
    }

    public static void setResultAsPayload(MessageContext msgContext, ListObjectResult result) {

        OMElement resultElement = generateOperationResult(msgContext, result);
        if (result.getResultEle() != null) {
            resultElement.addChild(result.getResultEle());
        }
        SOAPBody soapBody = msgContext.getEnvelope().getBody();
        //Detaching first element (soapBody.getFirstElement().detach()) will be done by following method anyway.
        JsonUtil.removeJsonPayload(((Axis2MessageContext) msgContext).getAxis2MessageContext());
        ((Axis2MessageContext) msgContext).getAxis2MessageContext().
                                          removeProperty(PassThroughConstants.NO_ENTITY_BODY);
        soapBody.addChild(resultElement);
    }

    public static OMElement generateOperationResult(MessageContext msgContext, ListObjectResult result) {
        //Create a new payload body and add to context

        String resultElementName = result.getOperation() + "Result";
        OMElement resultElement = createOMElement(resultElementName, null);

        OMElement statusCodeElement = createOMElement("success",
                                                      String.valueOf(result.isSuccessful()));
        resultElement.addChild(statusCodeElement);

        if (result.getWrittenBytes() != 0) {
            OMElement writtenBytesEle = createOMElement("writtenBytes",
                                                        String.valueOf(result.getWrittenBytes()));
            resultElement.addChild(writtenBytesEle);
        }

        Error error = result.getError();
        if (error != null) {
            setErrorPropertiesToMessage(msgContext, result.getError());
            //set error code and detail to the message
            OMElement errorEle = createOMElement("error", error.getCode());
            OMElement errorCodeEle = createOMElement("code", error.getCode());
            OMElement errorMessageEle = createOMElement("message", error.getMessage());
            errorEle.addChild(errorCodeEle);
            errorEle.addChild(errorMessageEle);
            resultElement.addChild(errorCodeEle);
            //set error detail
            if (StringUtils.isNotEmpty(result.getErrorMessage())) {
                OMElement errorDetailEle = createOMElement("detail", result.getErrorMessage());
                resultElement.addChild(errorDetailEle);
            }
        }

        return resultElement;
    }

    public static void setErrorPropertiesToMessage(MessageContext messageContext, Error error) {

        messageContext.setProperty(Const.PROPERTY_ERROR_CODE, error.getCode());
        messageContext.setProperty(Const.PROPERTY_ERROR_MESSAGE, error.getMessage());
        Axis2MessageContext axis2smc = (Axis2MessageContext) messageContext;
        org.apache.axis2.context.MessageContext axis2MessageCtx = axis2smc.getAxis2MessageContext();
        axis2MessageCtx.setProperty(Const.STATUS_CODE, Const.HTTP_STATUS_500);
    }
}
