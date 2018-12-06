
package org.endeavourhealth.cohortmanager.querydocument.models;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;


/**
 * <p>Java class for codeSetValue complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="codeSetValue">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="code" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="term" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="dataType" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="parentType" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="baseType" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="present" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="valueFrom" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="valueTo" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="units" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="includeChildren" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="exclusion" type="{}codeSetValue" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "codeSetValue", propOrder = {
    "code",
    "term",
    "dataType",
    "parentType",
    "baseType",
    "present",
    "valueFrom",
    "valueTo",
    "units",
    "includeChildren",
    "exclusion"
})
public class CodeSetValue {

    @XmlElement(required = true)
    protected String code;
    @XmlElement(required = true)
    protected String term;
    @XmlElement(required = true)
    protected String dataType;
    @XmlElement(required = true)
    protected String parentType;
    @XmlElement(required = true)
    protected String baseType;
    @XmlElement(required = true)
    protected String present;
    @XmlElement(required = true)
    protected String valueFrom;
    @XmlElement(required = true)
    protected String valueTo;
    @XmlElement(required = true)
    protected String units;
    protected boolean includeChildren;
    protected List<CodeSetValue> exclusion;

    /**
     * Gets the value of the code property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCode() {
        return code;
    }

    /**
     * Sets the value of the code property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCode(String value) {
        this.code = value;
    }

    /**
     * Gets the value of the term property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTerm() {
        return term;
    }

    /**
     * Sets the value of the term property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTerm(String value) {
        this.term = value;
    }

    /**
     * Gets the value of the dataType property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * Sets the value of the dataType property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDataType(String value) {
        this.dataType = value;
    }

    /**
     * Gets the value of the parentType property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getParentType() {
        return parentType;
    }

    /**
     * Sets the value of the parentType property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setParentType(String value) {
        this.parentType = value;
    }

    /**
     * Gets the value of the baseType property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getBaseType() {
        return baseType;
    }

    /**
     * Sets the value of the baseType property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setBaseType(String value) {
        this.baseType = value;
    }

    /**
     * Gets the value of the present property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPresent() {
        return present;
    }

    /**
     * Sets the value of the present property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPresent(String value) {
        this.present = value;
    }

    /**
     * Gets the value of the valueFrom property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValueFrom() {
        return valueFrom;
    }

    /**
     * Sets the value of the valueFrom property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValueFrom(String value) {
        this.valueFrom = value;
    }

    /**
     * Gets the value of the valueTo property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValueTo() {
        return valueTo;
    }

    /**
     * Sets the value of the valueTo property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValueTo(String value) {
        this.valueTo = value;
    }

    /**
     * Gets the value of the units property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUnits() {
        return units;
    }

    /**
     * Sets the value of the units property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUnits(String value) {
        this.units = value;
    }

    /**
     * Gets the value of the includeChildren property.
     * 
     */
    public boolean isIncludeChildren() {
        return includeChildren;
    }

    /**
     * Sets the value of the includeChildren property.
     * 
     */
    public void setIncludeChildren(boolean value) {
        this.includeChildren = value;
    }

    /**
     * Gets the value of the exclusion property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the exclusion property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getExclusion().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link CodeSetValue }
     * 
     * 
     */
    public List<CodeSetValue> getExclusion() {
        if (exclusion == null) {
            exclusion = new ArrayList<CodeSetValue>();
        }
        return this.exclusion;
    }

}
