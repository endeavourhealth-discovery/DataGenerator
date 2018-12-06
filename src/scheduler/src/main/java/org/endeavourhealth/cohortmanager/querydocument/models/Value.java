
package org.endeavourhealth.cohortmanager.querydocument.models;

import javax.xml.bind.annotation.*;


/**
 * <p>Java class for value complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="value">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="constant" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="testRuleId" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;choice>
 *           &lt;element name="absoluteUnit" type="{}valueAbsoluteUnit"/>
 *           &lt;element name="relativeUnit" type="{}valueRelativeUnit"/>
 *         &lt;/choice>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "value", propOrder = {
    "constant",
    "absoluteUnit",
    "relativeUnit",
    "testField"
})
@XmlSeeAlso({
    ValueTo.class,
    ValueFrom.class
})
public class Value {

    @XmlElement(required = true)
    protected String constant;
    @XmlSchemaType(name = "string")
    protected ValueAbsoluteUnit absoluteUnit;
    @XmlSchemaType(name = "string")
    protected ValueRelativeUnit relativeUnit;
    @XmlSchemaType(name = "string")
    protected String testField;


    /**
     * Gets the value of the constant property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getConstant() {
        return constant;
    }

    /**
     * Sets the value of the constant property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setConstant(String value) {
        this.constant = value;
    }

    /**
     * Gets the value of the absoluteUnit property.
     * 
     * @return
     *     possible object is
     *     {@link ValueAbsoluteUnit }
     *     
     */
    public ValueAbsoluteUnit getAbsoluteUnit() {
        return absoluteUnit;
    }

    /**
     * Sets the value of the absoluteUnit property.
     * 
     * @param value
     *     allowed object is
     *     {@link ValueAbsoluteUnit }
     *     
     */
    public void setAbsoluteUnit(ValueAbsoluteUnit value) {
        this.absoluteUnit = value;
    }

    /**
     * Gets the value of the relativeUnit property.
     * 
     * @return
     *     possible object is
     *     {@link ValueRelativeUnit }
     *     
     */
    public ValueRelativeUnit getRelativeUnit() {
        return relativeUnit;
    }

    /**
     * Sets the value of the relativeUnit property.
     * 
     * @param value
     *     allowed object is
     *     {@link ValueRelativeUnit }
     *     
     */
    public void setRelativeUnit(ValueRelativeUnit value) {
        this.relativeUnit = value;
    }

    public String getTestField() {
        return testField;
    }

    public void setTestField(String value) {
        this.testField = value;
    }

}
