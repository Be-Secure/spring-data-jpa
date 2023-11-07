package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;

import java.util.Collection;

/**
 * Execute a JPA query for a Java {@link Collection}-returning repository method.
 */
class CollectionExecutor extends JpaQueryContextExecutor {

    public CollectionExecutor(AbstractJpaQueryContext queryContext) {
        super(queryContext);
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
        return query.getResultList();
    }
}
