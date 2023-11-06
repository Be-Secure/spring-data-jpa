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

import static org.springframework.data.jpa.repository.query.QueryUtils.*;
import static org.springframework.data.repository.query.parser.Part.Type.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceUnitUtil;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.JoinType;
import jakarta.persistence.metamodel.Attribute;
import jakarta.persistence.metamodel.Bindable;
import jakarta.persistence.metamodel.EntityType;
import jakarta.persistence.metamodel.ManagedType;
import jakarta.persistence.metamodel.PluralAttribute;
import jakarta.persistence.metamodel.SingularAttribute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.query.ParameterMetadataContextProvider.ParameterImpl;
import org.springframework.data.jpa.repository.query.ParameterMetadataContextProvider.ParameterMetadata;
import org.springframework.data.jpa.repository.support.JpaMetamodelEntityInformation;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An {@link AbstractJpaQueryContext} used to handle custom finders.
 *
 * @author Greg Turnquist
 */
class CustomFinderQueryContext extends AbstractJpaQueryContext {

	private static final Log LOG = LogFactory.getLog(CustomFinderQueryContext.class);

	private final JpaParameters parameters;
	private final PartTree tree;
	private final JpaMetamodelEntityInformation<?, Object> entityInformation;
	private final boolean recreationRequired;
	private EscapeCharacter escape;

	// private ParameterMetadataContextProvider provider;
	private QueryCreator queryCreator;
	private QueryCreator countQueryCreator;

	public CustomFinderQueryContext(JpaQueryMethod method, EntityManager entityManager, EscapeCharacter escape) {

		super(method, entityManager);

		this.escape = escape;
		this.parameters = method.getParameters();

		Class<?> domainClass = method.getEntityInformation().getJavaType();
		PersistenceUnitUtil persistenceUnitUtil = entityManager.getEntityManagerFactory().getPersistenceUnitUtil();
		this.entityInformation = new JpaMetamodelEntityInformation<>(domainClass, entityManager.getMetamodel(),
				persistenceUnitUtil);

		this.recreationRequired = parameters.hasDynamicProjection() || parameters.potentiallySortsDynamically()
				|| method.isScrollQuery();

		try {

			this.tree = new PartTree(method.getName(), domainClass);

			validate(tree, parameters, method.getName());

		} catch (Exception ex) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s; %s", method, ex.getMessage()), ex);
		}

		if (this.getQueryMethod().isScrollQuery()) {
			super.executor = new ScrollExecutor(this, this.tree.getSort(), new ScrollDelegate<>(entityInformation));
		} else if (this.tree.isDelete()) {
			super.executor = new DeleteExecutor(this);
		} else if (this.tree.isExistsProjection()) {
			super.executor = new ExistsExecutor(this);
		}

	}

	@Override
	protected ContextualQuery createQuery(JpaParametersParameterAccessor accessor) {

		ParameterMetadataContextProvider provider;

		if (accessor != null) {
			provider = new ParameterMetadataContextProvider(accessor, escape);
		} else {
			provider = new ParameterMetadataContextProvider(parameters, escape);
		}

		this.queryCreator = this.tree.isCountProjection() //
				? new CountQueryCreator(recreationRequired, accessor, provider) //
				: new QueryCreator(recreationRequired, accessor, accessor.getParameters(), provider);

		Sort dynamicSort = getDynamicSort(accessor);
		String query = queryCreator.createQuery(dynamicSort);

		LOG.debug(getQueryMethod().getName() + ": " + query);

		System.out.println(query);

		return new ContextualQuery.StringQuery(query);
	}

	Sort getDynamicSort(JpaParametersParameterAccessor accessor) {

		return accessor.getParameters().potentiallySortsDynamically() //
				? accessor.getSort() //
				: Sort.unsorted();
	}

	@Override
	protected Query createCountQuery(JpaParametersParameterAccessor accessor) {

		if (accessor != null) {

			ParameterMetadataContextProvider provider = new ParameterMetadataContextProvider(accessor, escape);
			this.countQueryCreator = new CountQueryCreator(recreationRequired, accessor, provider);
		} else {

			ParameterMetadataContextProvider provider = new ParameterMetadataContextProvider(parameters, escape);
			this.countQueryCreator = new CountQueryCreator(recreationRequired, parameters, provider);
		}

		String countQuery = countQueryCreator.createQuery();

		System.out.println(countQuery);

		TypedQuery<Long> jpaCountQuery = getEntityManager().createQuery(countQuery, Long.class);

		Query boundCountQuery = bindParameters(jpaCountQuery, accessor);

		return boundCountQuery;
	}

	@Override
	protected Query turnIntoJpaQuery(ContextualQuery query, JpaParametersParameterAccessor accessor) {

		ReturnedType returnedType = getQueryMethod().getResultProcessor().withDynamicProjection(accessor).getReturnedType();

		Query jpaQuery;

		if (returnedType.needsCustomConstruction()) {

			Class<?> typeToRead = returnedType.getReturnedType();

			if (typeToRead.isInterface()) {
				jpaQuery = getEntityManager().createQuery(query.getQuery(), typeToRead);
			} else {

				String selections = returnedType.getInputProperties().stream() //
						.map(property -> PropertyPath.from(property, returnedType.getDomainType())) //
						.map(path -> alias(returnedType) + "." + path.toDotPath() + " as " + path.toDotPath())//
						.collect(Collectors.joining(","));

				String classBasedDtoQuery = query.getQuery().replace("select " + alias(returnedType.getDomainType()),
						"select " + selections);

				LOG.debug(getQueryMethod().getName() + ": Switching to classbased DTO query " + classBasedDtoQuery);
				System.out.println("Switching to " + classBasedDtoQuery);

				jpaQuery = getEntityManager().createQuery(classBasedDtoQuery, typeToRead);
			}

		} else if (tree.isExistsProjection()) {

			EntityType<?> model = getEntityManager().getMetamodel().entity(returnedType.getDomainType());

			if (model.hasSingleIdAttribute()) {

				SingularAttribute<?, ?> id = model.getId(model.getIdType().getJavaType());

				String selections = String.format("%s.%s as %s", alias(returnedType), id.getName(), id.getName());
				String existsQuery = query.getQuery().replace("select " + alias(returnedType.getDomainType()),
						"select " + selections);

				LOG.debug(getQueryMethod().getName() + ": Switching to " + existsQuery);
				System.out.println("Switching to " + existsQuery);

				jpaQuery = getEntityManager().createQuery(existsQuery);
			} else {

				String selections = model.getIdClassAttributes().stream() //
						.map(id -> String.format("%s.%s as %s", alias(returnedType), id.getName(), id.getName())) //
						.collect(Collectors.joining(","));

				String existsQuery = query.getQuery().replace("select " + alias(returnedType.getDomainType()),
						"select " + selections);

				LOG.debug(getQueryMethod().getName() + ": Switching to " + existsQuery);
				System.out.println("Switching to " + existsQuery);

				jpaQuery = getEntityManager().createQuery(existsQuery);
			}

		} else {
			jpaQuery = getEntityManager().createQuery(query.getQuery());
		}

		return jpaQuery;
	}

	@Override
	protected Class<?> getTypeToRead(ReturnedType type) {
		return tree.isDelete() ? type.getDomainType() : type.getTypeToRead();
	}

	@Override
	protected Query bindParameters(Query query, JpaParametersParameterAccessor accessor) {

		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata("query", query);

		ParameterBinder binder = ParameterBinderFactory.oneFlowBinder((JpaParameters) accessor.getParameters(),
				queryCreator.provider.getExpressions());

		if (binder == null) {
			throw new IllegalStateException("ParameterBinder is null");
		}

		Query boundQuery = binder.bindAndPrepare(query, metadata, accessor);

		ScrollPosition scrollPosition = accessor.getParameters().hasScrollPositionParameter()//
				? accessor.getScrollPosition() //
				: null;

		if (scrollPosition instanceof OffsetScrollPosition offset) {
			query.setFirstResult(Math.toIntExact(offset.getOffset()));
		}

		if (tree.isLimiting()) {
			if (query.getMaxResults() != Integer.MAX_VALUE) {
				/*
				 * In order to return the correct results, we have to adjust the first result offset to be returned if:
				 * - a Pageable parameter is present
				 * - AND the requested page number > 0
				 * - AND the requested page size was bigger than the derived result limitation via the First/Top keyword.
				 */
				if (query.getMaxResults() > tree.getMaxResults() && query.getFirstResult() > 0) {
					query.setFirstResult(query.getFirstResult() - (query.getMaxResults() - tree.getMaxResults()));
				}
			}

			query.setMaxResults(tree.getMaxResults());
		}

		if (tree.isExistsProjection()) {
			query.setMaxResults(1);
		}

		return boundQuery;
	}

	private void validate(PartTree tree, JpaParameters parameters, String methodName) {

		int argCount = 0;

		Iterable<Part> parts = () -> tree.stream().flatMap(Streamable::stream).iterator();

		for (Part part : parts) {

			int numberOfArguments = part.getNumberOfArguments();

			for (int i = 0; i < numberOfArguments; i++) {

				throwExceptionOnArgumentMismatch(methodName, part, parameters, argCount);

				argCount++;
			}
		}
	}

	private static void throwExceptionOnArgumentMismatch(String methodName, Part part, JpaParameters parameters,
			int index) {

		Part.Type type = part.getType();
		String property = part.getProperty().toDotPath();

		if (!parameters.getBindableParameters().hasParameterAt(index)) {
			throw new IllegalStateException(String.format(
					"Method %s expects at least %d arguments but only found %d; This leaves an operator of type %s for property %s unbound",
					methodName, index + 1, index, type.name(), property));
		}

		JpaParameters.JpaParameter parameter = parameters.getBindableParameter(index);

		if (expectsCollection(type) && !parameterIsCollectionLike(parameter)) {
			throw new IllegalStateException(wrongParameterTypeMessage(methodName, property, type, "Collection", parameter));
		} else if (!expectsCollection(type) && !parameterIsScalarLike(parameter)) {
			throw new IllegalStateException(wrongParameterTypeMessage(methodName, property, type, "scalar", parameter));
		}
	}

	private static boolean expectsCollection(Part.Type type) {
		return type == Part.Type.IN || type == Part.Type.NOT_IN;
	}

	private static boolean parameterIsCollectionLike(JpaParameters.JpaParameter parameter) {
		return Iterable.class.isAssignableFrom(parameter.getType()) || parameter.getType().isArray();
	}

	private static String wrongParameterTypeMessage(String methodName, String property, Part.Type operatorType,
			String expectedArgumentType, JpaParameters.JpaParameter parameter) {

		return String.format("Operator %s on %s requires a %s argument, found %s in method %s", operatorType.name(),
				property, expectedArgumentType, parameter.getType(), methodName);
	}

	/**
	 * Arrays are may be treated as collection like or in the case of binary data as scalar
	 */
	private static boolean parameterIsScalarLike(JpaParameters.JpaParameter parameter) {
		return !Iterable.class.isAssignableFrom(parameter.getType());
	}

	record Expression(List<String> joins, List<String> criteria) {

		public Expression() {
			this(new ArrayList<>(), new ArrayList<>());
		}

		public Expression join(String join) {

			this.joins.add(join);
			return new Expression(this.joins, this.criteria);
		}

		public Expression criteria(String criteria) {

			this.criteria.add(criteria);
			return new Expression(this.joins, this.criteria);
		}

		public Expression prependCriteria(String prefix) {
			return alterCriteria(criteria -> prefix + criteria);
		}

		public Expression appendCriteria(String suffix) {
			return alterCriteria(criteria -> criteria + suffix);
		}

		public Expression wrap(String prefix, String suffix) {
			return alterCriteria(criteria -> prefix + criteria) //
					.alterCriteria(criteria -> criteria + suffix);
		}

		private Expression alterCriteria(Function<String, String> transformer) {

			if (this.criteria.isEmpty()) {
				this.criteria.add("");
			}

			String originalCriteria = this.criteria.get(this.criteria.size() - 1);
			String newCriteria = transformer.apply(originalCriteria);
			this.criteria.remove(this.criteria.size() - 1);
			this.criteria.add(newCriteria);

			return this;
		}

		public String getJoin() {

			return joins.isEmpty() //
					? "" //
					: joins.get(joins.size() - 1);
		}

		public String getCriteria() {

			return criteria.isEmpty() //
					? "" //
					: criteria.get(criteria.size() - 1);
		}
	}

	static class CustomFinderUtils {

		public static CustomFinderUtils INSTANCE = new CustomFinderUtils();

		private Expression toExpression(EntityType<?> model, Expression expression, PropertyPath property) {
			return toExpression(model, expression, property, false);
		}

		private Expression toExpression(EntityType<?> model, Expression expression, PropertyPath property,
				boolean isForSelection) {
			return toExpression(model, expression, property, isForSelection, false);
		}

		private Expression toExpression(EntityType<?> model, Expression expression, PropertyPath property,
				boolean isForSelection, boolean hasRequiredOuterJoin) {

			String segment = property.getSegment();

			boolean isLeafProperty = !property.hasNext();

			boolean requiresOuterJoin = requiresOuterJoin(model, expression, property, isForSelection, hasRequiredOuterJoin);

			if (!requiresOuterJoin && isLeafProperty) {

				String alias;
				if (expression.joins.isEmpty()) {
					alias = alias(model.getJavaType());
				} else {
					String[] parts = expression.getJoin().split(" ");
					alias = parts[parts.length - 1];
				}

				if (expression.criteria.isEmpty()) {
					return expression.appendCriteria(alias + "." + segment);
				}

				return expression.appendCriteria("." + segment);

				// return alias.isEmpty() //
				// ? expression.criteria(segment) //
				// : expression.appendCriteria(alias + "." + segment);
			}

			JoinType joinType = requiresOuterJoin ? JoinType.LEFT : JoinType.INNER;
			Expression join = createJoin(model, expression, segment, joinType).criteria(segment);

			if (isLeafProperty) {
				return join;
			}

			PropertyPath nextProperty = Objects.requireNonNull(property.next(), "An element of the property path is null");

			return toExpression(model, join, nextProperty, isForSelection, requiresOuterJoin);
		}

		private Expression createJoin(EntityType<?> model, Expression expression, String segment, JoinType joinType) {

			String joinPrefix = joinType == JoinType.INNER ? "join " : "left outer join ";

			return expression.join(joinPrefix + alias(model.getJavaType()) + "." + segment + " as " + segment);
		}

		private static boolean requiresOuterJoin(EntityType<?> model, Expression expression, PropertyPath property,
				boolean isForSelection, boolean hasRequiredOuterJoin) {

			String segment = property.getSegment();

			if (isAlreadyInnerJoined(model, segment)) {
				return false;
			}

			ManagedType<?> managedType = model;
			Bindable<?> propertyPathModel = (Bindable<?>) managedType.getAttribute(segment);

			boolean isPluralAttribute = model instanceof PluralAttribute;
			boolean isLeafProperty = !property.hasNext();

			if (propertyPathModel == null && isPluralAttribute) {
				return true;
			}

			if (!(propertyPathModel instanceof Attribute<?, ?> attribute)) {
				return false;
			}

			// not a persistent attribute type association (@OneToOne, @ManyToOne)
			if (!ASSOCIATION_TYPES.containsKey(attribute.getPersistentAttributeType())) {
				return false;
			}

			boolean isCollection = attribute.isCollection();
			// if this path is an optional one to one attribute navigated from the not owning side we also need an
			// explicit outer join to avoid https://hibernate.atlassian.net/browse/HHH-12712
			// and https://github.com/eclipse-ee4j/jpa-api/issues/170
			boolean isInverseOptionalOneToOne = Attribute.PersistentAttributeType.ONE_TO_ONE == attribute
					.getPersistentAttributeType() && !getAnnotationProperty(attribute, "mappedBy", "").isEmpty();

			if (isLeafProperty && !isForSelection && !isCollection && !isInverseOptionalOneToOne && !hasRequiredOuterJoin) {
				return false;
			}

			return hasRequiredOuterJoin || getAnnotationProperty(attribute, "optional", true);
		}

		private static boolean isAlreadyInnerJoined(EntityType<?> model, String segment) {
			return false;
		}

	}

	private class QueryCreator extends AbstractQueryCreator<String, Expression> {

		private final boolean recreationRequired;

		private final Parameters<?, ?> parameters;
		private final ParameterMetadataContextProvider provider;
		protected final ReturnedType returnedType;
		private final JpaParametersParameterAccessor accessor;
		private final EntityType<?> model;

		public QueryCreator(boolean recreationRequired, JpaParametersParameterAccessor accessor,
				ParameterMetadataContextProvider provider) {
			this(recreationRequired, accessor, accessor.getParameters(), provider);
		}

		public QueryCreator(boolean recreationRequired, Parameters<?, ?> parameters,
				ParameterMetadataContextProvider provider) {
			this(recreationRequired, null, parameters, provider);
		}

		private QueryCreator(boolean recreationRequired, JpaParametersParameterAccessor accessor,
				Parameters<?, ?> parameters, ParameterMetadataContextProvider provider) {

			super(tree, accessor);

			this.accessor = accessor;

			this.recreationRequired = recreationRequired;
			this.parameters = parameters;
			this.provider = provider;
			this.returnedType = getQueryMethod().getResultProcessor().withDynamicProjection(accessor).getReturnedType();
			this.model = getEntityManager().getMetamodel().entity(returnedType.getDomainType());

		}

		@Override
		protected Expression create(Part part, Iterator<Object> iterator) {
			return toPredicate(part);
		}

		@Override
		protected Expression and(Part part, Expression base, Iterator<Object> iterator) {
			return base.appendCriteria(" and " + toPredicate(part).getCriteria());
		}

		@Override
		protected Expression or(Expression base, Expression predicate) {
			return base.appendCriteria(" or " + predicate.getCriteria());
		}

		@Override
		protected String complete(@Nullable Expression expression, @Nullable Sort sort) {

			String query = String.format(queryTemplate(), alias(returnedType), entityName(returnedType), alias(returnedType));

			if (expression != null) {

				if (!expression.joins.isEmpty()) {
					query += " " + expression.joins.stream() //
							.collect(Collectors.joining(" "));
				}

				if (!expression.criteria.isEmpty()) {
					query += " where " + expression.criteria.stream() //
							.collect(Collectors.joining(","));
				}
			}

			query += orderByClause(sort);

			LOG.debug(query);

			return query;

		}

		protected String queryTemplate() {
			return "select %s from %s %s";
		}

		protected String orderByClause(@Nullable Sort sort) {

			if (sort != null && sort.isSorted()) {
				return " order by " + String.join(",", QueryUtils.toOrders(sort, returnedType.getDomainType()));
			}

			return "";
		}

		private Expression toPredicate(Part part) {
			return new PredicateBuilder(part, provider, model).build();
		}

	}

	private static Map<Class<?>, String> entityNameCache = new HashMap<>();

	private static Map<String, String> aliasCache = new HashMap<>();

	/**
	 * Extract a JPQL entity name for a given {@link ReturnedType}.
	 * 
	 * @param returnedType
	 * @return
	 */
	private String entityName(ReturnedType returnedType) {
		return entityName(returnedType.getDomainType());
	}

	/**
	 * Extract a JPQL entity name for a given {@link Class}.
	 * 
	 * @param entityType
	 * @return
	 */
	private String entityName(Class<?> entityType) {
		return entityNameCache.computeIfAbsent(entityType,
				clazz -> getEntityManager().getMetamodel().entity(clazz).getName());
	}

	private static String baseEntityName(Class<?> entityType) {
		return entityType.getSimpleName();
	}

	/**
	 * Extract a JPQL alias for a given {@link ReturnedType}.
	 * 
	 * @param returnedType
	 * @return
	 */
	private String alias(ReturnedType returnedType) {
		return alias(returnedType.getDomainType());
	}

	/**
	 * Extract a JPQL alias for a given {@link Class}.
	 * 
	 * @param clazz
	 * @return
	 */
	private static String alias(Class<?> clazz) {
		return aliasCache.computeIfAbsent(baseEntityName(clazz), entityName -> entityName.substring(0, 1).toLowerCase());
	}

	private class CountQueryCreator extends QueryCreator {
		public CountQueryCreator(boolean recreationRequired, JpaParametersParameterAccessor accessor,
				ParameterMetadataContextProvider provider) {
			super(recreationRequired, accessor, provider);
		}

		public CountQueryCreator(boolean recreationRequired, Parameters<?, ?> parameters,
				ParameterMetadataContextProvider provider) {
			super(recreationRequired, parameters, provider);
		}

		@Override
		protected String queryTemplate() {
			return "select count(%s) from %s %s";
		}

		@Override
		protected String orderByClause(Sort sort) {
			return "";
		}

	}

	private class PredicateBuilder {
		private final Part part;
		private final EntityType<?> model;

		ParameterMetadataContextProvider provider;

		public PredicateBuilder(Part part, ParameterMetadataContextProvider provider, EntityType<?> model) {

			Assert.notNull(part, "Part must not be null");
			Assert.notNull(provider, "Provider must not be null");

			this.part = part;
			this.provider = provider;
			this.model = model;
		}

		public Expression build() {

			PropertyPath property = part.getProperty();
			Part.Type type = part.getType();

			String simpleAlias = alias(property.getOwningType().getType());

			switch (type) {
				case BETWEEN:
					ParameterMetadata<Object> first = provider.next(part);
					ParameterMetadata<Object> second = provider.next(part);
					return getComparablePath(part).appendCriteria(" between " + first.getValue() + " and " + second.getValue());
				case AFTER:
				case GREATER_THAN:
					return getComparablePath(part).appendCriteria(" > " + provider.next(part, Comparable.class).getValue());
				case GREATER_THAN_EQUAL:
					return getComparablePath(part).appendCriteria(" >= " + provider.next(part, Comparable.class).getValue());
				case BEFORE:
				case LESS_THAN:
					return getComparablePath(part).appendCriteria(" < " + provider.next(part, Comparable.class).getValue());
				case LESS_THAN_EQUAL:
					return getComparablePath(part).appendCriteria(" <= " + provider.next(part, Comparable.class).getValue());
				case IS_NULL:
					return getTypedPath(part).appendCriteria(" IS NULL");
				case IS_NOT_NULL:
					return getTypedPath(part).appendCriteria(" IS NOT NULL");
				case NOT_IN:
					return upperIfIgnoreCase(getTypedPath(part))
							.appendCriteria(" NOT IN " + provider.next(part, Collection.class).getValue());
				case IN:
					return upperIfIgnoreCase(getTypedPath(part))
							.appendCriteria(" IN " + provider.next(part, Collection.class).getValue());
				case STARTING_WITH:
				case ENDING_WITH:
				case CONTAINING:
				case NOT_CONTAINING:

					if (property.getLeafProperty().isCollection()) {

						String propertyExpression = traversePath(simpleAlias, property);
						ParameterImpl<Object> parameterExpression = provider.next(part).getExpression();

						return type.equals(NOT_CONTAINING) //
								? isNotMember(parameterExpression, propertyExpression) //
								: isMember(parameterExpression, propertyExpression);
					}

				case LIKE:
				case NOT_LIKE:

					Expression propertyExpression = upperIfIgnoreCase(getTypedPath(part));
					Expression parameterExpression = upperIfIgnoreCase(provider.next(part, String.class).getValue());

					return type.equals(NOT_LIKE) || type.equals(NOT_CONTAINING)
							? propertyExpression.appendCriteria(" NOT LIKE ").appendCriteria(parameterExpression.getCriteria())
							: propertyExpression.appendCriteria(" LIKE ").appendCriteria(parameterExpression.getCriteria());
				case TRUE:
					return getTypedPath(part).appendCriteria(" IS TRUE");
				case FALSE:
					return getTypedPath(part).appendCriteria(" IS FALSE");
				case SIMPLE_PROPERTY:

					ParameterMetadata<Object> expression = provider.next(part);

					return expression.isIsNullParameter() //
							? getTypedPath(part).appendCriteria(" IS NULL") //
							: upperIfIgnoreCase(getTypedPath(part))
									.appendCriteria(" = " + upperIfIgnoreCase(expression.getValue()).getCriteria());

				case NEGATING_SIMPLE_PROPERTY:
					return upperIfIgnoreCase(getTypedPath(part)).appendCriteria(" <> ")
							.appendCriteria(upperIfIgnoreCase(provider.next(part).getValue()).getCriteria());
				case IS_EMPTY:
				case IS_NOT_EMPTY:

					if (!property.getLeafProperty().isCollection()) {
						throw new IllegalArgumentException("IsEmpty / IsNotEmpty can only be used on collection properties");
					}

					String collectionPath = traversePath("", property);

					return type.equals(IS_NOT_EMPTY) //
							? isNotEmpty(collectionPath) //
							: isEmpty(collectionPath);
				default:
					throw new IllegalArgumentException("Unsupported keyword " + type);
			}
		}

		private Expression upperIfIgnoreCase(String value) {
			return upperIfIgnoreCase(new Expression().criteria(value));
		}

		private Expression upperIfIgnoreCase(Expression expression) {

			switch (part.shouldIgnoreCase()) {

				case ALWAYS:

					Assert.state(canUpperCase(part.getProperty()),
							"Unable to ignore case of " + part.getProperty().getType().getName() + " types, the property '"
									+ part.getProperty().getSegment() + "' must reference a String");

					return expression.wrap("upper(", ")");

				case WHEN_POSSIBLE:

					if (canUpperCase(part.getProperty())) {
						return expression.wrap("upper(", ")");
					}

				case NEVER:
				default:
					return expression;
			}
		}

		private boolean canUpperCase(PropertyPath property) {
			return String.class.equals(property.getType());
		}

		private Expression isNotMember(ParameterImpl<Object> parameterExpression, String propertyExpression) {
			return new Expression().criteria(parameterExpression.getValue() + " NOT MEMBER OF " + propertyExpression);
		}

		private Expression isMember(ParameterImpl<Object> parameterExpression, String propertyExpression) {
			return new Expression().criteria(parameterExpression.getValue() + " MEMBER OF " + propertyExpression);
		}

		private Expression isNotEmpty(String collectionPath) {
			return new Expression().criteria(collectionPath + " is not empty");
		}

		private Expression isEmpty(String collectionPath) {
			return new Expression().criteria(collectionPath + " is empty");
		}

		private Expression getComparablePath(Part part) {
			return getTypedPath(part);
		}

		private Expression getTypedPath(Part part) {
			return CustomFinderUtils.INSTANCE.toExpression(model, new Expression(), part.getProperty());
		}

		private String traversePath(String totalPath, PropertyPath path) {

			String result = totalPath.isEmpty() ? path.getSegment() : totalPath + "." + path.getSegment();

			return path.hasNext() //
					? traversePath(result, path.next()) //
					: result;
		}
	}
}
