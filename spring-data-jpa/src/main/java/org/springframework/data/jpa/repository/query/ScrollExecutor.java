package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;

/**
 * Execute a JPA query for a repository method using the Scroll API.
 */
class ScrollExecutor extends JpaQueryContextExecutor {
    private final Sort sort;
    private final ScrollDelegate<?> delegate;

    public ScrollExecutor(AbstractJpaQueryContext queryContext, Sort sort, ScrollDelegate<?> delegate) {

        super(queryContext);

        this.sort = sort;
        this.delegate = delegate;
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

        ScrollPosition scrollPosition = accessor.getScrollPosition();

        return delegate.scroll(query, sort.and(accessor.getSort()), scrollPosition);
    }
}
