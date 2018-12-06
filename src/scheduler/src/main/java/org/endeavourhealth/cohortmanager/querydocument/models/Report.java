
package org.endeavourhealth.cohortmanager.querydocument.models;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;


/**
 * <p>Java class for report complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="report">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="cohortFeature" type="{}reportCohortFeature" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="lastRunDate" type="{http://www.w3.org/2001/XMLSchema}long" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "report", propOrder = {
    "cohortFeature",
    "lastRunDate"
})
public class Report {

    protected List<ReportCohortFeature> cohortFeature;
    protected Long lastRunDate;

    /**
     * Gets the value of the cohortFeature property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the cohortFeature property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getCohortFeature().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ReportCohortFeature }
     * 
     * 
     */
    public List<ReportCohortFeature> getCohortFeature() {
        if (cohortFeature == null) {
            cohortFeature = new ArrayList<ReportCohortFeature>();
        }
        return this.cohortFeature;
    }

    /**
     * Gets the value of the lastRunDate property.
     * 
     * @return
     *     possible object is
     *     {@link Long }
     *     
     */
    public Long getLastRunDate() {
        return lastRunDate;
    }

    /**
     * Sets the value of the lastRunDate property.
     * 
     * @param value
     *     allowed object is
     *     {@link Long }
     *     
     */
    public void setLastRunDate(Long value) {
        this.lastRunDate = value;
    }

}
