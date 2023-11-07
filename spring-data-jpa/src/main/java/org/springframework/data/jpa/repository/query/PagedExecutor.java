package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;
import org.springframework.data.support.PageableExecutionUtils;

import java.util.List;

/**
 * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Page}-returning repository method.
 */
class PagedExecutor extends JpaQueryContextExecutor {

    public PagedExecutor(AbstractJpaQueryContext queryContext) {
        super(queryContext);
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
        return PageableExecutionUtils.getPage(query.getResultList(), accessor.getPageable(),
                () -> count(queryContext, accessor));
    }

    private long count(AbstractJpaQueryContext queryContext, JpaParametersParameterAccessor accessor) {

        Query countQuery = queryContext.createCountQuery(accessor);

        List<?> totals = countQuery.getResultList();

        return totals.size() == 1 //
                ? CONVERSION_SERVICE.convert(totals.get(0), Long.class) //
                : totals.size();
    }

}
