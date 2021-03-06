package org.endeavourhealth.cohortmanager;

import org.endeavourhealth.cohortmanager.models.*;
import org.endeavourhealth.cohortmanager.querydocument.models.*;
import org.endeavourhealth.cohortmanager.json.*;
import org.endeavourhealth.cohortmanager.querydocument.models.Rule;
import org.endeavourhealth.cohortmanager.querydocument.models.RuleAction;
import org.endeavourhealth.scheduler.models.PersistenceManager;
import org.endeavourhealth.scheduler.models.database.CohortResultsEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import java.sql.SQLOutput;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class CohortManager {

	private static final Logger LOG = LoggerFactory.getLogger(CohortManager.class);

	public static Timestamp convertToDate(String date) {
		Timestamp timeStamp = null;

		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date parsedDate = dateFormat.parse(date);
			timeStamp = new Timestamp(parsedDate.getTime());
		} catch (Exception e) {

		}

		return timeStamp;
	}

	private static String getDenominatorSQL(String cohortPopulation) {
		if (cohortPopulation.equals("0")) // currently registered
			return "select distinct p.id " +
					"from ceg_compass_data.patient p JOIN ceg_compass_data.episode_of_care e on e.patient_id = p.id " +
					"where p.date_of_death IS NULL and p.organization_id IN (?0) " +
					"and e.registration_type_id = 2 " +
					"and e.date_registered <= NOW() " +
					"and (e.date_registered_end > NOW() or e.date_registered_end IS NULL)";
		else if (cohortPopulation.equals("1")) // all patients
			return "select distinct p.id " +
					"from ceg_compass_data.patient p where p.organization_id IN (?0)";
		return "";
	}

	public static void runCohort(LibraryItem libraryItem, Integer extractId) throws Exception {

		List<QueryResult> queryResults = new ArrayList<>();
		String organisations = libraryItem.getOrganisations();
		executeRules(libraryItem, queryResults, organisations);
		calculateAndStoreResults(libraryItem, queryResults, extractId, organisations);
	}

	private static List<Integer> calculateAndStoreResults(LibraryItem libraryItem, List<QueryResult> queryResults, Integer extractId, String organisations) throws Exception {
		Integer ruleId = libraryItem.getQuery().getStartingRules().getRuleId().get(0);

		Integer i = 0;

		String denominatorSQL = getDenominatorSQL("0");

		EntityManager entityManager = PersistenceManager.getEntityManager();

		Query query = entityManager.createNativeQuery(denominatorSQL);
		List<String> selectedValues = Arrays.asList(organisations.split("\\s*,\\s*"));
		query.setParameter(0, selectedValues);

		List<Object> patients = query.getResultList();

		// get the denominator list of patients (needed for FAILED rule conditions)
		List<Integer> denominatorPatients = new ArrayList<>();
		for (Object patientEntity : patients) {
			denominatorPatients.add((Integer)patientEntity);
		}

		entityManager.close();

		List<Integer> finalPatients = calculateCohortFromRules(queryResults, denominatorPatients, ruleId, i);

		saveIdentifiedPatientsIntoCohortPatientTable(finalPatients, extractId);

		return finalPatients;
	}

	private static void saveIdentifiedPatientsIntoCohortPatientTable(List<Integer> finalPatients, Integer extractId) throws Exception {
		// save each patient identified into the query cohort patient table
		for (Integer patient : finalPatients) {
			CohortResultsEntity cohortPatientsEntity = new CohortResultsEntity();
			cohortPatientsEntity.setOrganisationId(0); // TODO not sure we need org ID?
			Integer patientId = patient;
			cohortPatientsEntity.setExtractId(extractId);
			cohortPatientsEntity.setPatientId(patientId);

			cohortPatientsEntity.saveCohortPatients(cohortPatientsEntity);
		}
	}

	private static List<Integer> calculateCohortFromRules(List<QueryResult> queryResults, List<Integer> denominatorPatients, Integer ruleId, Integer i) throws Exception {
		List<Integer> finalPatients = new ArrayList<>();

		while (true) { // loop through all the rules
			i++;

			RuleAction rulePassAction = getRuleAction(true, ruleId, queryResults);
			RuleAction ruleFailAction = getRuleAction(false, ruleId, queryResults);

			List<Integer> patients1 = getPatientsInRule(ruleId, queryResults);  // get patients in rule

			if (i == 1 && ruleFailAction != null && ruleFailIncludeGoto(ruleFailAction)) {

				List<Integer> denominatorPatients1 = new ArrayList<Integer>(denominatorPatients);
				denominatorPatients1.removeAll(patients1); // fail action so calculate patients from denominator who have not met the rule's conditions
				patients1 = new ArrayList<Integer>(denominatorPatients1);

			} else if (i > 1 && rulePassAction != null && ruleFailAction != null && (rulePassIncludeGoto(rulePassAction) || ruleFailIncludeGoto(ruleFailAction))) {

				if (ruleFailIncludeGoto(ruleFailAction))
					finalPatients.removeAll(patients1); // fail action so remove patients not matching criteria
				else
					finalPatients.retainAll(patients1); // narrow down to patients common in both lists

				if (finalPatients.isEmpty())
					break;
			}

			if (rulePassAction != null && ruleFailAction != null && rulePassFailGoto(rulePassAction, ruleFailAction)) { // Rule passes and moves to next rule

				ruleId = getNextRuleIds(rulePassAction, ruleFailAction).get(0);

				List<Integer> patients2 = getPatientsInRule(ruleId, queryResults);  // get patients in rule

				rulePassAction = getRuleAction(true, ruleId, queryResults);
				ruleFailAction = getRuleAction(false, ruleId, queryResults);

				if (finalPatients.isEmpty())
					finalPatients = new ArrayList<Integer>(patients1); // first rule

				if (ruleFailAction != null && ruleFailIncludeGoto(ruleFailAction))
					finalPatients.removeAll(patients2); // fail action so remove patients not matching criteria
				else
					finalPatients.retainAll(patients2); // narrow down to patients common in both lists

				if (finalPatients.isEmpty())
					break;

				if (rulePassAction != null && ruleFailAction != null && rulePassFailGoto(rulePassAction, ruleFailAction))  // Rule passes and moves to next rule
					ruleId = getNextRuleIds(rulePassAction, ruleFailAction).get(0);

				else if (rulePassAction != null && ruleFailAction != null && rulePassFailInclude(rulePassAction, ruleFailAction)) // Rule includes patients, so break out the loop
					break;

			} else if (rulePassAction != null && ruleFailAction != null && rulePassFailInclude(rulePassAction, ruleFailAction)) { // Rule includes patients, so break out the loop

				if (finalPatients.isEmpty()) // Only one rule in Query
					finalPatients = new ArrayList<Integer>(patients1);

				break;
			}

		}

		return finalPatients;
	}

	private static void executeRules(LibraryItem libraryItem, List<QueryResult> queryResults, String organisations) throws Exception {

		for (Rule rule : libraryItem.getQuery().getRule()) { // execute each rule
			List<Filter> filters = rule.getTest().getFilter();

			QueryMeta q = new QueryMeta();

			buildFilters(filters, q);

			runRule(libraryItem, queryResults, rule, q, filters, organisations);
		} // next Rule in Query

	}

	private static void runRule(LibraryItem libraryItem, List<QueryResult> queryResults, Rule rule, QueryMeta q, List<Filter> filters, String organisations) throws Exception {
		String cohortPopulation = "0";

		String restriction = "LATEST";

		if (rule.getTest().getRestriction()!=null)
			restriction = rule.getTest().getRestriction().getRestriction();

		String ruleSQL = getRuleSQL(cohortPopulation, q, restriction);

		EntityManager entityManager = PersistenceManager.getEntityManager();

		List<Object[]> patients = null;
		List<Object[]> patientObservations = new ArrayList<>();
		List<Object[]> patientObservations2 = new ArrayList<>();
		List<Object[]> patientObservationsCompare = new ArrayList<>();
		List<Object[]> patientMedicationStatements = new ArrayList<>();
		List<Object[]> patientAllergy = new ArrayList<>();
		List<Object[]> patientReferral = new ArrayList<>();


		if (rule.getType()==3) { // Test rule
			String field = "";
			String valueFrom = "";
			String valueTo = "";
			Date dateFrom = null;
			Date dateTo = null;
			String testField = "";
			String relativeUnit = "";
			String codes = "";

			for (Filter filter : filters) {
				field = filter.getField();
				if (field.contains("VALUE")||field.contains("CLINICAL_DATE")) {
					if (filter.getValueFrom() != null) {
						valueFrom = filter.getValueFrom().getConstant();
						testField = filter.getValueFrom().getTestField();
						if (filter.getValueFrom().getRelativeUnit() != null) {
							relativeUnit = filter.getValueFrom().getRelativeUnit().value();
							valueFrom = "-" + valueFrom;
							dateFrom = getRelativeDateFromBaseline(relativeUnit,new Date(),valueFrom);
						}
					}
					if (filter.getValueTo() != null) {
						valueTo = filter.getValueTo().getConstant();
						testField = filter.getValueTo().getTestField();
						if (filter.getValueTo().getRelativeUnit() != null) {
							relativeUnit = filter.getValueTo().getRelativeUnit().value();
							valueTo = "-" + valueTo;
							dateTo = getRelativeDateFromBaseline(relativeUnit,new Date(),valueTo);
						}
					}
				}
				else if (field.contains("CODE")) {
					codes = buildConceptList(filter);
				}
			} // next Filter

			Integer ruleId = getRuleForTest(libraryItem, field);
			patientObservations = getRuleObservations(ruleId, queryResults);

			if (!testField.equals("BASELINE_DATE") && !testField.equals("")) {
				ruleId = getRuleForTest(libraryItem, testField);
				patientObservationsCompare = getRuleObservations(ruleId, queryResults);
			}

			Integer i = 0;
			for (Object[] observationEntity : patientObservations) {
				if (!valueFrom.equals("") && !valueTo.equals("")) {
					if (field.contains("CLINICAL_DATE")) {
						if (!testField.equals("BASELINE_DATE")) {
							Integer patientId = (Integer)observationEntity[0]; // patientId
							dateFrom = null;
							dateTo = null;
							for (Object[] observationCompareEntity : patientObservationsCompare) {
								if (observationCompareEntity[0]==patientId) {
									Date compareDate = (Date)observationCompareEntity[1]; // effective date
									if (compareDate!=null) {
										dateFrom = getRelativeDateFromBaseline(relativeUnit,compareDate,valueFrom);
										dateTo = getRelativeDateFromBaseline(relativeUnit,compareDate,valueTo);
									}
									break;
								}
							}
						}
						if (observationEntity[1]!=null &&
								dateFrom!=null && dateTo!=null &&
								((Date)observationEntity[1]).after(dateFrom) &&
								((Date)observationEntity[1]).before(dateTo)) {
							patientObservations2.add(observationEntity);
						}
					} else if (field.contains("VALUE")) {

							patientObservations2.add(observationEntity);

					}
				} else if (!valueFrom.equals("") && valueTo.equals("")) {
					if (field.contains("CLINICAL_DATE")) {
						if (!testField.equals("BASELINE_DATE")) {
							Integer patientId = (Integer)observationEntity[0];
							dateFrom = null;
							for (Object[] observationCompareEntity : patientObservationsCompare) {
								if (observationCompareEntity[0]==patientId) {
									Date compareDate = (Date)observationCompareEntity[1];
									if (compareDate!=null)
										dateFrom = getRelativeDateFromBaseline(relativeUnit,compareDate,valueFrom);
									break;
								}
							}
						}
						if (observationEntity[1]!=null &&
								dateFrom!=null &&
								((Date)observationEntity[1]).after(dateFrom)) {
							patientObservations2.add(observationEntity);
						}
					} else if (field.contains("VALUE")) {

							patientObservations2.add(observationEntity);

					}
				} else if (valueFrom.equals("") && !valueTo.equals("")) {
					if (field.contains("CLINICAL_DATE")) {
						if (!testField.equals("BASELINE_DATE")) {
							Integer patientId = (Integer)observationEntity[0];
							dateTo = null;
							for (Object[] observationCompareEntity : patientObservationsCompare) {
								if (observationCompareEntity[0]==patientId) {
									Date compareDate = (Date)observationCompareEntity[1];
									if (compareDate!=null)
										dateTo = getRelativeDateFromBaseline(relativeUnit,compareDate,valueTo);
									break;
								}
							}
						}
						if (observationEntity[1]!=null &&
								dateTo!=null &&
								((Date)observationEntity[1]).before(dateTo)) {
							patientObservations2.add(observationEntity);
						}
					} else if (field.contains("VALUE")) {

							patientObservations2.add(observationEntity);

					}
				} else if (field.contains("CODE")) {
					if (codes.contains(observationEntity[2].toString()+","))
						patientObservations2.add(observationEntity);
				}

				i++;
			}
			patientObservations = new ArrayList<Object[]>(patientObservations2);
			patientObservations2 = new ArrayList<>();

		} else if (rule.getType()==1) { // Feature rule
			if (ruleSQL.contains("JOIN ceg_compass_data.observation")) {
				Query query = entityManager.createNativeQuery(ruleSQL);
				List<String> selectedValues = Arrays.asList(organisations.split("\\s*,\\s*"));
				query.setParameter(0, selectedValues);
				setQueryParams(query, q.whereParams);
				patientObservations = query.getResultList();
			} else if (ruleSQL.contains("JOIN ceg_compass_data.medication_statement")) {
				Query query = entityManager.createNativeQuery(ruleSQL);
				List<String> selectedValues = Arrays.asList(organisations.split("\\s*,\\s*"));
				query.setParameter(0, selectedValues);
				setQueryParams(query, q.whereParams);
				patientMedicationStatements = query.getResultList();

			} else if (ruleSQL.contains("JOIN ceg_compass_data.allergy_intolerance")) {
				Query query = entityManager.createNativeQuery(ruleSQL);
				List<String> selectedValues = Arrays.asList(organisations.split("\\s*,\\s*"));
				query.setParameter(0, selectedValues);
				setQueryParams(query, q.whereParams);
				patientAllergy = query.getResultList();

			} else if (ruleSQL.contains("JOIN ceg_compass_data.referral_request")) {
				Query query = entityManager.createNativeQuery(ruleSQL);
				List<String> selectedValues = Arrays.asList(organisations.split("\\s*,\\s*"));
				query.setParameter(0, selectedValues);
				setQueryParams(query, q.whereParams);
				patientReferral = query.getResultList();

			} else if (ruleSQL.contains("JOIN ceg_compass_data.patient")) {
				Query query = entityManager.createNativeQuery(ruleSQL);
				List<String> selectedValues = Arrays.asList(organisations.split("\\s*,\\s*"));
				query.setParameter(0, selectedValues);
				setQueryParams(query, q.whereParams);
				patients = query.getResultList();
			}
		}

		// add the rule's identified list of patients to the overall Query Result list
		QueryResult queryResult = new QueryResult();
		queryResult.setOrganisationId(0);
		queryResult.setRuleId(rule.getId());
		queryResult.setOnPass(rule.getOnPass());
		queryResult.setOnFail(rule.getOnFail());
		List<Integer> queryPatients = new ArrayList<>();

		if (ruleSQL.contains("JOIN ceg_compass_data.patient")) {
			Integer patientId = 0;
			for (Object patientEntity : patients) {
				patientId = (Integer)patientEntity;
				if (queryPatients.indexOf(patientId)<0) // only add distinct patients
					queryPatients.add(patientId);
			}
		} else if (patientObservations.size()>0) {
			Integer patientId = 0;
			Integer lastPatientId = 0;
			for (Object[] observationEntity : patientObservations) {
				patientId = (Integer)observationEntity[0];
				if (queryPatients.indexOf(patientId)<0) { // only add distinct patients
					queryPatients.add((Integer)observationEntity[0]);
				}
				if (!patientId.equals(lastPatientId)) {
					patientObservations2.add(observationEntity);
				}
				lastPatientId = patientId;
			}
		} else if (patientMedicationStatements.size()>0) {
			Integer patientId = 0;
			Integer lastPatientId = 0;
			for (Object[] medicationStatementEntity : patientMedicationStatements) {
				patientId = (Integer)medicationStatementEntity[0];
				if (queryPatients.indexOf(patientId)<0) // only add distinct patients
					queryPatients.add((Integer)medicationStatementEntity[0]);
				//if (!patientId.equals(lastPatientId)) {
					//patientObservations2.add(medicationOrderEntity);
				//}
				lastPatientId = patientId;
			}
		} else if (patientAllergy.size()>0) {
			Integer patientId = 0;
			Integer lastPatientId = 0;
			for (Object[] AllergyEntity : patientAllergy) {
				patientId = (Integer)AllergyEntity[0];
				if (queryPatients.indexOf(patientId)<0) // only add distinct patients
					queryPatients.add((Integer)AllergyEntity[0]);
				//if (!patientId.equals(lastPatientId)) {
				//patientObservations2.add(medicationOrderEntity);
				//}
				lastPatientId = patientId;
			}
		} else if (patientReferral.size()>0) {
			Integer patientId = 0;
			Integer lastPatientId = 0;
			for (Object[] ReferralEntity : patientReferral) {
				patientId = (Integer)ReferralEntity[0];
				if (queryPatients.indexOf(patientId)<0) // only add distinct patients
					queryPatients.add((Integer)ReferralEntity[0]);
				//if (!patientId.equals(lastPatientId)) {
				//patientObservations2.add(medicationOrderEntity);
				//}
				lastPatientId = patientId;
			}
		}
		queryResult.setPatients(queryPatients);
		queryResult.setObservations(patientObservations2);
		queryResults.add(queryResult);

		LOG.info("Running cohort: " + libraryItem.getName());

		entityManager.close();
	}

	private static void buildFilters(List<Filter> filters, QueryMeta q) throws Exception {
		for (Filter filter : filters) { // build the SQL for each filter
			String field = filter.getField();

			switch (field) {
				case "CONCEPT":
					buildConceptFilter(q, filter);
					break;
				case "EFFECTIVE_DATE":
					buildEffectiveDateFilter(q, filter);
					break;
				case "OBSERVATION_PROBLEM":
					q.sqlWhere += " and d.is_problem = '1'";
					break;
				case "MEDICATION_STATUS":
					q.sqlWhere += " and d.is_active = '1'";
					break;
				case "MEDICATION_TYPE":
					buildMedicationTypeFilter(q, filter);
					break;

			}

		} // next Filter
	}

	private static void buildMedicationTypeFilter(QueryMeta q, Filter filter) {
		for (String value : filter.getValueSet().getValue()) {
			switch (value) {
				case "ACUTE":
					q.sqlWhere += " or d.medication_statement_authorisation_type_id = '0'";
					break;
				case "REPEAT":
					q.sqlWhere += " or d.medication_statement_authorisation_type_id = '1'";
					break;
				case "REPEAT_DISPENSING":
					q.sqlWhere += " or d.medication_statement_authorisation_type_id = '2'";
					break;
				case "AUTOMATIC":
					q.sqlWhere += " or d.medication_statement_authorisation_type_id = '3'";
					break;
			}
		}
		q.sqlWhere = q.sqlWhere.replaceFirst("or d.medication_statement_authorisation_type_id", "and (d.medication_statement_authorisation_type_id");
		q.sqlWhere += ")";
	}

	private static void buildEffectiveDateFilter(QueryMeta q, Filter filter) throws Exception {
		if (filter.getValueFrom() != null) {
			String dateFrom = filter.getValueFrom().getConstant();
			if (filter.getValueFrom().getRelativeUnit() != null) {
				dateFrom = "-" + dateFrom;
				dateFrom = getRelativeDateFromBaselineAsString(filter.getValueFrom().getRelativeUnit().value(),new Date(),dateFrom);
			}
			q.sqlWhere += " and d.clinical_effective_date >= " + parameterize(q.whereParams, convertToDate(dateFrom));
		} else if (filter.getValueTo() != null) {
			String dateTo = filter.getValueTo().getConstant();
			if (filter.getValueTo().getRelativeUnit() != null) {
				dateTo = "-" + dateTo;
				dateTo = getRelativeDateFromBaselineAsString(filter.getValueTo().getRelativeUnit().value(),new Date(),dateTo);
			}
			q.sqlWhere += " and d.clinical_effective_date <= " + parameterize(q.whereParams, convertToDate(dateTo));
		}
	}

	private static void adjustCalendar(String dateFrom, String relativeUnit, Calendar calDate) {
		switch (relativeUnit) {
			case "day":
				calDate.add(Calendar.DATE, Integer.parseInt(dateFrom));
				break;
			case "week":
				calDate.add(Calendar.DATE, Integer.parseInt(dateFrom) * 7);
				break;
			case "month":
				calDate.add(Calendar.MONTH, Integer.parseInt(dateFrom));
				break;
			case "year":
				calDate.add(Calendar.YEAR, Integer.parseInt(dateFrom));
				break;
		}
	}

	private static void buildConceptFilter(QueryMeta q, Filter filter) {
		List<CodeSetValue> codeSetValues = filter.getCodeSet().getCodeSetValue();
		Integer c = 0;
		for (CodeSetValue codeSetValue : codeSetValues) {
			c++;
			String code = codeSetValue.getCode();
			String term = codeSetValue.getTerm();
			String parentType = codeSetValue.getParentType();
			String baseType = codeSetValue.getBaseType();
			String valueFrom = codeSetValue.getValueFrom();
			String valueTo = codeSetValue.getValueTo();
			Boolean includeChildren = codeSetValue.isIncludeChildren();

			buildConceptTypeFilter(q, c, code, term, parentType, baseType, valueFrom, valueTo);
		}

		if (!q.dataTable.equals("ceg_compass_data.patient"))
			q.sqlWhere = "and (" + q.sqlWhere + ")";

	}

	private static String buildConceptList(Filter filter) {
		List<CodeSetValue> codeSetValues = filter.getCodeSet().getCodeSetValue();
		String codes = "";
		for (CodeSetValue codeSetValue : codeSetValues) {
			String code = codeSetValue.getCode();
			codes += code+",";
		}
		return codes;
	}

	private static void buildConceptTypeFilter(QueryMeta q, Integer c, String code, String term, String parentType, String baseType, String valueFrom, String valueTo) {
		switch (baseType) {
			case "Patient":
				buildConceptPatientFilter(q, term, parentType, valueFrom, valueTo);
				break;
			case "Observation":
				q.patientJoinField = "patient_id";
				q.codesetTypeJoinField = "read2_concept_id";
				q.dataTable = "ceg_compass_data.observation";
				buildCodeSetFilter(q, c, code, valueFrom, valueTo);
				break;
			case "Medication Statement":
				q.patientJoinField = "patient_id";
                q.codesetTypeJoinField = "sct_concept_id";
				q.dataTable = "ceg_compass_data.medication_statement";
				buildCodeSetFilter(q, c, code, valueFrom, valueTo);
                break;
			case "Medication Order":
				q.patientJoinField = "patient_id";
                q.codesetTypeJoinField = "sct_concept_id";
				q.dataTable = "ceg_compass_data.medication_order";
				buildCodeSetFilter(q, c, code, valueFrom, valueTo);
                break;
			case "Allergy":
				q.patientJoinField = "patient_id";
                q.codesetTypeJoinField = "read2_concept_id";
				q.dataTable = "ceg_compass_data.allergy_intolerance";
				buildCodeSetFilter(q, c, code, valueFrom, valueTo);
                break;
			case "Referral":
				q.patientJoinField = "patient_id";
                q.codesetTypeJoinField = "read2_concept_id";
				q.dataTable = "ceg_compass_data.referral_request";
				buildCodeSetFilter(q, c, code, valueFrom, valueTo);
                break;
			case "Encounter":
				q.patientJoinField = "patient_id";
                q.codesetTypeJoinField = "read2_concept_id";
				q.dataTable = "ceg_compass_data.encounter";
				buildCodeSetFilter(q, c, code, valueFrom, valueTo);
                break;
		}
	}

	private static void buildCodeSetFilter(QueryMeta q, Integer c, String code, String valueFrom, String valueTo) {
		String pref = " or";
		if (c == 1)
			pref = "";

		if (valueFrom.equals("") && valueTo.equals(""))
			q.sqlWhere += pref + " c.code_set_id = " + parameterize(q.whereParams, code);

	}

	private static void buildConceptPatientFilter(QueryMeta q, String term, String parentType, String valueFrom, String valueTo) {
		q.patientJoinField = "id";
		q.dataTable = "ceg_compass_data.patient";
		if (term.equals("Male")) {
			q.sqlWhere += " and p.patient_gender_id = '0'";
		} else if (term.equals("Female")) {
			q.sqlWhere += " and p.patient_gender_id = '1'";
		} else if (term.equals("Date of Death")) {
			if (!valueFrom.equals("") && !valueTo.equals(""))
				q.sqlWhere += " and p.date_of_death between " + parameterize(q.whereParams, convertToDate(valueFrom))+" and "+ parameterize(q.whereParams, convertToDate(valueTo));
			else if (!valueFrom.equals("") && valueTo.equals(""))
				q.sqlWhere += " and p.date_of_death >= " + parameterize(q.whereParams, convertToDate(valueFrom));
			else if (valueFrom.equals("") && !valueTo.equals(""))
				q.sqlWhere += " and p.date_of_death <= " + parameterize(q.whereParams, convertToDate(valueTo));
		} else if (term.equals("Date of Birth")) {
			if (!valueFrom.equals("") && !valueTo.equals(""))
				q.sqlWhere += " and p.date_of_birth between " + parameterize(q.whereParams, convertToDate(valueFrom))+" and "+ parameterize(q.whereParams, convertToDate(valueTo));
			else if (!valueFrom.equals("") && valueTo.equals(""))
				q.sqlWhere += " and p.date_of_birth >= " + parameterize(q.whereParams, convertToDate(valueFrom));
			else if (valueFrom.equals("") && !valueTo.equals(""))
				q.sqlWhere += " and p.date_of_birth <= " + parameterize(q.whereParams, convertToDate(valueTo));
		} else if (term.equals("Age")) {
			if (!valueFrom.equals("") && !valueTo.equals(""))
				q.sqlWhere += " and DATEDIFF(NOW(), p.date_of_birth) / 365.25 between " + parameterize(q.whereParams, valueFrom)+" and "+ parameterize(q.whereParams, valueTo);
			else if (!valueFrom.equals("") && valueTo.equals(""))
				q.sqlWhere += " and DATEDIFF(NOW(), p.date_of_birth) / 365.25 >= " + parameterize(q.whereParams, valueFrom);
			else if (valueFrom.equals("") && !valueTo.equals(""))
				q.sqlWhere += " and DATEDIFF(NOW(), p.date_of_birth) / 365.25 <= " + parameterize(q.whereParams, valueTo);
		}
	}

	public static RuleAction getRuleAction(Boolean pass, Integer ruleId, List<QueryResult> queryResults) throws Exception {

		RuleAction ruleAction = null;

		for (QueryResult qr : queryResults) {
			if (qr.getRuleId().intValue() == ruleId.intValue()) {
				if (pass)
					ruleAction = qr.getOnPass();
				else
					ruleAction = qr.getOnFail();
				break;
			}
		}

		return ruleAction;
	}

	public static List<Integer> getPatientsInRule(Integer ruleId, List<QueryResult> queryResults) throws Exception {

		List<Integer> patients = null;

		for (QueryResult qr : queryResults) {
			if (qr.getRuleId().intValue() == ruleId.intValue()) {
				patients = qr.getPatients();  // get patients in rule
				break;
			}
		}

		return patients;
	}

	public static List<Object[]> getRuleObservations(Integer ruleId, List<QueryResult> queryResults) throws Exception {

		List<Object[]> observationEntities = null;

		for (QueryResult qr : queryResults) {
			if (qr.getRuleId().intValue() == ruleId.intValue()) {
				observationEntities = qr.getObservations();  // get observations in rule
				break;
			}
		}

		return observationEntities;
	}

	public static List<Integer> getNextRuleIds(RuleAction rulePassAction, RuleAction ruleFailAction) throws Exception {

		List<Integer> nextRuleIds = new ArrayList();
		if (rulePassAction.getAction().equals(RuleActionOperator.GOTO_RULES))
			nextRuleIds = rulePassAction.getRuleId();
		else if (ruleFailAction.getAction().equals(RuleActionOperator.GOTO_RULES))
			nextRuleIds = ruleFailAction.getRuleId();

		return nextRuleIds;

	}

	public static Boolean ruleFailIncludeGoto(RuleAction ruleFailAction) throws Exception {

		Boolean result = false;

		if (ruleFailAction.getAction().equals(RuleActionOperator.INCLUDE) ||
				ruleFailAction.getAction().equals(RuleActionOperator.GOTO_RULES))
			result = true;

		return result;
	}

	public static Boolean rulePassIncludeGoto(RuleAction rulePassAction) throws Exception {

		Boolean result = false;

		if (rulePassAction.getAction().equals(RuleActionOperator.INCLUDE) ||
				rulePassAction.getAction().equals(RuleActionOperator.GOTO_RULES))
			result = true;

		return result;
	}

	public static Boolean rulePassFailGoto(RuleAction rulePassAction, RuleAction ruleFailAction) throws Exception {

		Boolean result = false;

		if (rulePassAction.getAction().equals(RuleActionOperator.GOTO_RULES) ||
				ruleFailAction.getAction().equals(RuleActionOperator.GOTO_RULES))
			result = true;

		return result;
	}

	public static Boolean rulePassFailInclude(RuleAction rulePassAction, RuleAction ruleFailAction) throws Exception {

		Boolean result = false;

		if (rulePassAction.getAction().equals(RuleActionOperator.INCLUDE) ||
				ruleFailAction.getAction().equals(RuleActionOperator.INCLUDE))
			result = true;

		return result;
	}

	public static String getRuleSQL(String cohortPopulation, QueryMeta q, String restriction) {
		String order = "DESC";
		if (restriction.equals("LATEST"))
			order = "DESC";
		else if (restriction.equals("EARLIEST"))
			order = "ASC";

		if (cohortPopulation.equals("0")) { // currently registered
			String sql = "";
			if (q.dataTable.equals("ceg_compass_data.patient")) {
				sql = "select p.id " +
						"from ceg_compass_data.patient p JOIN ceg_compass_data.episode_of_care e on e.patient_id = p.id " +
						"JOIN " + q.dataTable + " d on d." + q.patientJoinField + " = p.id " +
						"where p.date_of_death IS NULL and p.organization_id IN (?0) " +
						"and e.registration_type_id = 2 " +
						"and e.date_registered <= NOW() " +
						"and (e.date_registered_end > NOW() or e.date_registered_end IS NULL) "+q.sqlWhere;
			} else {
				sql = "select d.patient_id, d.effective_date, d.original_code " +
						"from ceg_compass_data.patient p JOIN ceg_compass_data.episode_of_care e on e.patient_id = p.id " +
						"JOIN " + q.dataTable + " d on d." + q.patientJoinField + " = p.id " +
                        "JOIN subscriber_transform_pcr.code_set_codes c on c." + q.codesetTypeJoinField + " = d.original_code " +
						"where p.date_of_death IS NULL and p.organization_id IN (?0) " +
						"and e.registration_type_id = 2 " +
						"and e.date_registered <= NOW() " +
						"and (e.date_registered_end > NOW() or e.date_registered_end IS NULL) "+q.sqlWhere+
						" order by p.id, d.clinical_effective_date "+order;
			}
            // System.out.println(sql);
			return sql;
		} else if (cohortPopulation.equals("1")) { // all patients
			String sql = "";
			if (q.dataTable.equals("ceg_compass_data.patient")) {
				sql = "select p.id " +
						"from ceg_compass_data.patient p " +
						"JOIN " + q.dataTable + " d on d." + q.patientJoinField + " = p.id " +
						"where p.organization_id IN (?0) "+q.sqlWhere;
			} else {
				sql = "select d.patient_id, d.effective_date, d.original_code " +
						"from ceg_compass_data.patient p " +
						"JOIN " + q.dataTable + " d on d." + q.patientJoinField + " = p.id " +
                        "JOIN subscriber_transform_pcr.code_set_codes c on c." + q.codesetTypeJoinField + " = d.original_code " +
						"where p.organization_id IN (?0) "+q.sqlWhere+
						" order by p.id, d.clinical_effective_date "+order;
			}
            // System.out.println(sql);
			return sql;
		}

		return "";
	}

	private static Integer getRuleForTest(LibraryItem libraryItem, String fieldToMatch) throws Exception {
		Integer ruleId = 0;
		for (Rule rule : libraryItem.getQuery().getRule()) {
			if (rule.getTest().getRestriction()!=null) {
				String prefix = rule.getTest().getRestriction().getPrefix();
				fieldToMatch = fieldToMatch.split("-")[0];
				if (prefix.equals(fieldToMatch)) {
					ruleId = rule.getId();
					break;
				}
			}
		}

		return ruleId;
	}

	private static Date getRelativeDateFromBaseline(String relativeUnit, Date baselineDate, String value) throws Exception {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calDate = Calendar.getInstance(TimeZone.getTimeZone("Europe/London"));
		dateFormat.setCalendar(calDate);
		calDate.setTime(baselineDate);
		adjustCalendar(value, relativeUnit, calDate);
		return calDate.getTime();
	}

	private static String getRelativeDateFromBaselineAsString(String relativeUnit, Date baselineDate, String value) throws Exception {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calDate = Calendar.getInstance(TimeZone.getTimeZone("Europe/London"));
		dateFormat.setCalendar(calDate);
		calDate.setTime(baselineDate);
		adjustCalendar(value, relativeUnit, calDate);
		return dateFormat.format(calDate.getTime());
	}

	public static void setQueryParams(Query q, List<Object> params) {
		for (int i = 0; i < params.size(); i++) {
			if (params.get(i) instanceof Calendar)
				q.setParameter(i + 1, (Calendar) params.get(i), TemporalType.TIMESTAMP);
			else
				q.setParameter(i + 1, params.get(i));
		}
	}

	private static String parameterize(List<Object> list, Object value) {
		list.add(value);
		return " ?" + (list.size()) + " ";
	}
}