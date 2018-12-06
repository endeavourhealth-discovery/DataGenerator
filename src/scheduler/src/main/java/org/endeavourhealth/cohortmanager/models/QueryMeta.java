package org.endeavourhealth.cohortmanager.models;

import java.util.ArrayList;
import java.util.List;

public class QueryMeta {
	public String sqlWhere = "";
	public String dataTable = "";
	public String patientJoinField = "";
	public List<Object> whereParams = new ArrayList<>();
}
