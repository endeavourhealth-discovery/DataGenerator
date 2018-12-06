
package org.endeavourhealth.cohortmanager.querydocument.models;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for reportCohortFeature complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="reportCohortFeature">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="cohortFeatureUuid" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="fieldName" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "reportCohortFeature", propOrder = {
    "cohortFeatureUuid",
    "fieldName"
})
public class ReportCohortFeature {

    @XmlElement(required = true)
    protected String cohortFeatureUuid;
    @XmlElement(required = true)
    protected String fieldName;

    /**
     * Gets the value of the cohortFeatureUuid property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCohortFeatureUuid() {
        return cohortFeatureUuid;
    }

    /**
     * Sets the value of the cohortFeatureUuid property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCohortFeatureUuid(String value) {
        this.cohortFeatureUuid = value;
    }

    /**
     * Gets the value of the fieldName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the value of the fieldName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setFieldName(String value) {
        this.fieldName = value;
    }

}
