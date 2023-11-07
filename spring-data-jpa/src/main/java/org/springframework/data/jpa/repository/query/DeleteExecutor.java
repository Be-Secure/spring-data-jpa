package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;

import java.util.List;

/**
 * Execute a JPA delete.
 */
class DeleteExecutor extends JpaQueryContextExecutor {
    public DeleteExecutor(AbstractJpaQueryContext queryContext) {
        super(queryContext);
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

        List<?> resultList = query.getResultList();

        for (Object o : resultList) {
            queryContext.getEntityManager().remove(o);
        }

        return queryContext.getQueryMethod().isCollectionQuery() ? resultList : resultList.size();
    }
}
