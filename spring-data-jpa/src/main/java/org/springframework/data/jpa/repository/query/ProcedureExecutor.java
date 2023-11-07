package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;
import jakarta.persistence.StoredProcedureQuery;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.repository.core.support.SurroundingTransactionDetectorMethodInterceptor;
import org.springframework.util.Assert;

/**
 * Execute a JPA stored procedure.
 */
class ProcedureExecutor extends JpaQueryContextExecutor {

	private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a @Procedure method without a surrounding transaction that keeps the connection open so that the ResultSet can actually be consumed; Make sure the consumer code uses @Transactional or any other way of declaring a (read-only) transaction";
	private final StoredProcedureQueryContext storedProcedureContext;

	public ProcedureExecutor(AbstractJpaQueryContext context) {

		super(context);

		Assert.isInstanceOf(StoredProcedureQueryContext.class, context);
		this.storedProcedureContext = (StoredProcedureQueryContext) context;
	}

	@Override
	protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

		Assert.isInstanceOf(StoredProcedureQuery.class, query);
		StoredProcedureQuery procedure = (StoredProcedureQuery) query;

		try {
			boolean returnsResultSet = procedure.execute();

			if (returnsResultSet) {

				if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
					throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
				}

				return storedProcedureContext.getQueryMethod().isCollectionQuery() ? procedure.getResultList()
						: procedure.getSingleResult();
			}

			return storedProcedureContext.extractOutputValue(procedure); // extract output value from the procedure
		} finally {
			if (procedure instanceof AutoCloseable autoCloseable) {
				try {
					autoCloseable.close();
				} catch (Exception ignored) {}
			}
		}
	}
}
