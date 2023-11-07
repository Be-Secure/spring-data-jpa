/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jpa.repository.query;

import static org.springframework.data.jpa.repository.query.CustomFinderQueryContext.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.metamodel.EntityType;

import java.util.Optional;
import java.util.function.Consumer;

import org.springframework.data.domain.Example;

public class QueryByExampleQueryContext<T> extends AbstractJpaQueryContext {

	private final Example<T> example;
	private final QueryByExampleExpressionBuilder.Predicate predicate;

	private Consumer<Query> queryHints;

	public QueryByExampleQueryContext(EntityManager entityManager, Example<T> example) {

		super(Optional.empty(), entityManager);

		this.example = example;
		this.predicate = QueryByExampleExpressionBuilder
				.getPredicate(entityManager.getMetamodel().entity(example.getProbeType()), example);
	}

	@Override
	protected ContextualQuery createQuery(JpaParametersParameterAccessor accessor) {

		EntityType<?> model = getEntityManager().getMetamodel().entity(example.getProbeType());

		String alias = alias(example.getProbeType());
		String entityName = model.getName();
		String query = String.format("select %s from %s %s", alias, entityName, alias);

		QueryByExampleExpressionBuilder.Predicate predicate = QueryByExampleExpressionBuilder.getPredicate(model, example);

		if (predicate != null) {

			String predicates = predicate.getClause();

			if (!predicates.isEmpty()) {
				query += " where " + predicates;
			}
		}

		return ContextualQuery.of(query);
	}

	@Override
	protected Query turnIntoJpaQuery(ContextualQuery query, JpaParametersParameterAccessor accessor) {

		Class<?> typeToRead = example.getProbeType();

		return getEntityManager().createQuery(query.getQuery(), typeToRead);
	}

	@Override
	protected Query applyQueryHints(Query query) {

		queryHints.accept(query);
		return query;
	}

	public void registerQueryHints(Consumer<Query> queryHints) {
		this.queryHints = queryHints;
	}
}
