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
package vn.ds.study.utils;

import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.util.AXIOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OMElementUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(OMElementUtils.class);

    private OMElementUtils() {

    }

    public static OMElement createOMElement(final String elementName,final Object value) {
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

}
