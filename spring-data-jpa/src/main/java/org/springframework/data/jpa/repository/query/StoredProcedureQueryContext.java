package org.springframework.data.jpa.repository.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.ParameterMode;
import jakarta.persistence.Query;
import jakarta.persistence.StoredProcedureQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.repository.query.Parameter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

class StoredProcedureQueryContext extends AbstractJpaQueryContext {

	private final StoredProcedureAttributes procedureAttributes;
	private final boolean useNamedParameters;

	public StoredProcedureQueryContext(JpaQueryMethod method, EntityManager entityManager) {

		super(method, entityManager);
		this.procedureAttributes = method.getProcedureAttributes();
		this.useNamedParameters = method.getParameters().stream() //
				.anyMatch(Parameter::isNamedParameter);
	}

	@Override
	protected String createQuery(JpaParametersParameterAccessor accessor) {
		return procedureAttributes.getProcedureName();
	}

	@Override
	protected Query turnIntoJpaQuery(String query, JpaParametersParameterAccessor accessor) {

		return procedureAttributes.isNamedStoredProcedure() //
				? newNamedStoredProcedureQuery(query)
				: newAdhocStoredProcedureQuery(query);
	}

	@Override
	protected Query bindParameters(Query query, JpaParametersParameterAccessor accessor) {

		Assert.isInstanceOf(StoredProcedureQuery.class, query);
		StoredProcedureQuery storedProcedure = (StoredProcedureQuery) query;

		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata("singleton", storedProcedure);

		return parameterBinder.get().bind(storedProcedure, metadata, accessor);
	}

	@Nullable
	Object extractOutputValue(StoredProcedureQuery storedProcedureQuery) {

		Assert.notNull(storedProcedureQuery, "StoredProcedureQuery must not be null");

		if (!procedureAttributes.hasReturnValue()) {
			return null;
		}

		List<ProcedureParameter> outputParameters = procedureAttributes.getOutputProcedureParameters();

		if (outputParameters.size() == 1) {
			return extractOutputParameterValue(outputParameters.get(0), 0, storedProcedureQuery);
		}

		Map<String, Object> outputValues = new HashMap<>();

		for (int i = 0; i < outputParameters.size(); i++) {
			ProcedureParameter outputParameter = outputParameters.get(i);
			outputValues.put(outputParameter.getName(),
					extractOutputParameterValue(outputParameter, i, storedProcedureQuery));
		}

		return outputValues;
	}

	/**
	 * @return The value of an output parameter either by name or by index.
	 */
	private Object extractOutputParameterValue(ProcedureParameter outputParameter, Integer index,
			StoredProcedureQuery storedProcedureQuery) {

		JpaParameters methodParameters = getQueryMethod().getParameters();

		return useNamedParameters && StringUtils.hasText(outputParameter.getName())
				? storedProcedureQuery.getOutputParameterValue(outputParameter.getName())
				: storedProcedureQuery.getOutputParameterValue(methodParameters.getNumberOfParameters() + index + 1);
	}

	private Query newNamedStoredProcedureQuery(String query) {
		return getEntityManager().createNamedStoredProcedureQuery(query);
	}

	private Query newAdhocStoredProcedureQuery(String query) {

		StoredProcedureQuery procedureQuery = getQueryMethod().isQueryForEntity() //
				? getEntityManager().createStoredProcedureQuery(query, getQueryMethod().getEntityInformation().getJavaType()) //
				: getEntityManager().createStoredProcedureQuery(query);

		JpaParameters params = (JpaParameters) getQueryMethod().getParameters();

		for (JpaParameters.JpaParameter param : params) {

			if (!param.isBindable()) {
				continue;
			}

			if (useNamedParameters) {
				procedureQuery.registerStoredProcedureParameter(
						param.getName()
								.orElseThrow(() -> new IllegalArgumentException(ParameterBinder.PARAMETER_NEEDS_TO_BE_NAMED)),
						param.getType(), ParameterMode.IN);
			} else {
				procedureQuery.registerStoredProcedureParameter(param.getIndex() + 1, param.getType(), ParameterMode.IN);
			}
		}

		if (procedureAttributes.hasReturnValue()) {

			ProcedureParameter procedureOutput = procedureAttributes.getOutputProcedureParameters().get(0);

			/*
			 * If there is a {@link java.sql.ResultSet} with a {@link ParameterMode#REF_CURSOR}, find the output parameter.
			 * Otherwise, no need, there is no need to find an output parameter.
			 */
			if (storedProcedureHasResultSetUsingRefCursor(procedureOutput) || !isResultSetProcedure()) {

				if (useNamedParameters) {
					procedureQuery.registerStoredProcedureParameter(procedureOutput.getName(), procedureOutput.getType(),
							procedureOutput.getMode());
				} else {

					// Output parameter should be after the input parameters
					int outputParameterIndex = params.getNumberOfParameters() + 1;

					procedureQuery.registerStoredProcedureParameter(outputParameterIndex, procedureOutput.getType(),
							procedureOutput.getMode());
				}
			}
		}

		return procedureQuery;
	}

	/**
	 * Does this stored procedure have a {@link java.sql.ResultSet} using {@link ParameterMode#REF_CURSOR}?
	 *
	 * @param procedureOutput
	 * @return
	 */
	private boolean storedProcedureHasResultSetUsingRefCursor(ProcedureParameter procedureOutput) {
		return isResultSetProcedure() && procedureOutput.getMode() == ParameterMode.REF_CURSOR;
	}

	/**
	 * @return true if the stored procedure will use a ResultSet to return data and not output parameters
	 */
	private boolean isResultSetProcedure() {
		return getQueryMethod().isCollectionQuery() || getQueryMethod().isQueryForEntity();
	}

}
