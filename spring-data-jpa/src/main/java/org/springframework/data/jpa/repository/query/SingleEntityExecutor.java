package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;

/**
 * Execute a JPA query for a repository method returning a single value.
 */
public class SingleEntityExecutor extends JpaQueryContextExecutor {

    public SingleEntityExecutor(AbstractJpaQueryContext queryContext) {
        super(queryContext);
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
        return query.getSingleResult();
    }
}
