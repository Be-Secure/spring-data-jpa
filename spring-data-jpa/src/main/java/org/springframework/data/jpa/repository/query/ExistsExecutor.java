package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;

/**
 * Execute a JPA exists operation.
 */
class ExistsExecutor extends JpaQueryContextExecutor {

    public ExistsExecutor(AbstractJpaQueryContext queryContext) {
        super(queryContext);
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
        return !query.getResultList().isEmpty();
    }
}
