package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;

import java.util.List;

/**
 * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Slice}-returning repository method.
 */
class SlicedExecutor extends JpaQueryContextExecutor {

    public SlicedExecutor(AbstractJpaQueryContext queryContext) {
        super(queryContext);
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

        Pageable pageable = accessor.getPageable();

        int pageSize = 0;
        if (pageable.isPaged()) {

            pageSize = pageable.getPageSize();
            query.setMaxResults(pageSize + 1);
        }

        List<?> resultList = query.getResultList();

        boolean hasNext = pageable.isPaged() && resultList.size() > pageSize;

        if (hasNext) {
            return new SliceImpl<>(resultList.subList(0, pageSize), pageable, hasNext);
        } else {
            return new SliceImpl<>(resultList, pageable, hasNext);
        }
    }
}
