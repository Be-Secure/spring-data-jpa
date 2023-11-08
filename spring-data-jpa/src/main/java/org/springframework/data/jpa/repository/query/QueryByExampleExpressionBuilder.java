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

import jakarta.persistence.metamodel.Attribute;
import jakarta.persistence.metamodel.Attribute.PersistentAttributeType;
import jakarta.persistence.metamodel.EntityType;
import jakarta.persistence.metamodel.ManagedType;
import jakarta.persistence.metamodel.SingularAttribute;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.ExampleMatcher.PropertyValueTransformer;
import org.springframework.data.support.ExampleMatcherAccessor;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Greg Turnquist
 */
public class QueryByExampleExpressionBuilder {

	private static final Set<PersistentAttributeType> ASSOCIATION_TYPES;

	static {
		ASSOCIATION_TYPES = EnumSet.of( //
				PersistentAttributeType.MANY_TO_MANY, //
				PersistentAttributeType.MANY_TO_ONE, //
				PersistentAttributeType.ONE_TO_MANY, //
				PersistentAttributeType.ONE_TO_ONE);
	}

	@Nullable
	public static <T> Predicate getPredicate(EntityType<?> model, Example<T> example) {
		return getPredicate(new Predicate(), model, example, EscapeCharacter.DEFAULT);
	}

	@Nullable
	static <T> Predicate getPredicate(Predicate root, EntityType<?> model, Example<T> example,
			EscapeCharacter escapeCharacter) {

		Assert.notNull(example, "Example must not be null");

		ExampleMatcher matcher = example.getMatcher();

		String alias = alias(model.getJavaType());

		List<Predicate> predicates = getPredicates("", root, model, alias, example.getProbe(), example.getProbeType(),
				new ExampleMatcherAccessor(matcher), new PathNode("root", null, example.getProbe()), escapeCharacter);

		if (predicates.isEmpty()) {
			return null;
		}

		if (predicates.size() == 1) {
			return predicates.iterator().next();
		}

		return matcher.isAllMatching() ? and(predicates) : or(predicates);
	}

	public static class Predicate {

		private final String clause;

		private final List<String> attributes;

		private final List<String> joins;

		public Predicate() {
			this("", new ArrayList<>(), new ArrayList<>());
		}

		public Predicate(String clause) {
			this(clause, new ArrayList<>(), new ArrayList<>());
		}

		private Predicate(String clause, List<String> attributes, List<String> joins) {

			this.clause = clause;
			this.attributes = attributes;
			this.joins = joins;
		}

		public Predicate attribute(String attribute) {

			attributes.add(attribute);
			return this;
		}

		public Predicate join(String name) {

			joins.add(name);
			return this;
		}

		public Predicate get(SingularAttribute attribute) {

			attributes.add(attribute.getName());

			if (attribute.isAssociation() || attribute.getPersistentAttributeType() == PersistentAttributeType.EMBEDDED) {

				System.out.println(attribute + " is associated with something!");
				return join(attribute.getName());
			}

			return this;
		}

		String getAttribute() {
			return attributes.isEmpty() ? "" : attributes.get(attributes.size() - 1);
		}

		public boolean attributeExists(String attribute) {
			return attributes.contains(attribute);
		}

		public boolean joinExists(String join) {
			return joins.contains(join);
		}

		public String build(String alias) {

			String results = joins.stream() //
					.map(join -> String.format("left outer join %s.%s as %s", alias, join, join))//
					.collect(Collectors.joining(" "));

			results += joins.isEmpty() ? "" : " ";
			results += clause.isEmpty() ? "" : "where " + clause;

			return results;
		}

		public Predicate clause(String clause) {
			return new Predicate(clause, attributes, joins);
		}

		public Predicate wrap(String before, String after) {
			return clause(before + this.clause + after);
		}

		public Predicate attributesToClause(String prefix, String suffix) {

			Predicate predicate = new Predicate(prefix + "." + String.join(".", attributes) + suffix, new ArrayList<>(),
					joins);

			this.attributes.clear();

			return predicate;
		}
	}

	private static abstract class CompositePredicate extends Predicate {

		CompositePredicate(List<Predicate> predicates, String joiner) {

			super(predicates.stream() //
					.map(predicate -> predicate.clause) //
					.collect(Collectors.joining(" " + joiner + " ")));

			predicates.forEach(predicate -> {

				predicate.attributes.forEach(attribute -> {
					if (!attributeExists(attribute)) {
						attribute(attribute);
					}
				});
				predicate.joins.forEach(join -> {
					if (!joinExists(join)) {
						join(join);
					}
				});
			});
		}
	}

	private static class AndPredicate extends CompositePredicate {

		public AndPredicate(List<Predicate> predicates) {
			super(predicates, "and");
		}
	}

	private static class OrPredicate extends CompositePredicate {

		public OrPredicate(List<Predicate> predicates) {
			super(predicates, "or");
		}
	}

	/**
	 * Join a collection of {@link Predicate}s into a single clause via an {@literal AND} operation.
	 *
	 * @param predicates
	 * @return
	 */
	private static Predicate and(List<Predicate> predicates) {
		return new AndPredicate(predicates);
	}

	/**
	 * Join a collection of {@link Predicate}s into a single clause via an {@literal OR} operation.
	 *
	 * @param predicates
	 * @return
	 */
	private static Predicate or(List<Predicate> predicates) {
		return new OrPredicate(predicates);
	}

	static List<Predicate> getPredicates(String path, Predicate from, ManagedType<?> type, String alias, Object value,
			Class<?> probeType, ExampleMatcherAccessor exampleAccessor, PathNode currentNode,
			EscapeCharacter escapeCharacter) {

		List<Predicate> predicates = new ArrayList<>();
		DirectFieldAccessFallbackBeanWrapper beanWrapper = new DirectFieldAccessFallbackBeanWrapper(value);

		for (SingularAttribute attribute : type.getSingularAttributes()) {

			String currentPath = !StringUtils.hasText(path) ? attribute.getName() : path + "." + attribute.getName();

			if (exampleAccessor.isIgnoredPath(currentPath)) {
				continue;
			}

			PropertyValueTransformer transformer = exampleAccessor.getValueTransformerForPath(currentPath);
			Optional<Object> optionalValue = transformer
					.apply(Optional.ofNullable(beanWrapper.getPropertyValue(attribute.getName())));

			if (!optionalValue.isPresent()) { // TODO: Switch to {@link Optional#isEmpty}

				if (exampleAccessor.getNullHandler().equals(ExampleMatcher.NullHandler.INCLUDE)) {
					predicates.add(isNull(alias, from.get(attribute)));
				}
				continue;
			}

			Object attributeValue = optionalValue.get();

			if (attributeValue == Optional.empty()) {
				continue;
			}

			if (attribute.getPersistentAttributeType().equals(PersistentAttributeType.EMBEDDED)
					|| (isAssociation(attribute))) {

				List<Predicate> predicates1 = getPredicates(currentPath, from.get(attribute),
						(ManagedType<?>) attribute.getType(), alias, attributeValue, probeType, exampleAccessor, currentNode,
						escapeCharacter);

				predicates.addAll(predicates1);

				continue;
			}

			if (isAssociation(attribute)) {

				PathNode node = currentNode.add(attribute.getName(), attributeValue);
				if (node.spansCycle()) {
					throw new InvalidDataAccessApiUsageException(
							String.format("Path '%s' from %s must not span a cyclic reference%n%s", currentNode,
									ClassUtils.getShortName(probeType), node));
				}

				predicates
						.addAll(getPredicates(currentPath, from.join(attribute.getName()), (ManagedType<?>) attribute.getType(),
								alias, attributeValue, probeType, exampleAccessor, node, escapeCharacter));

				continue;
			}

			if (attribute.getJavaType().equals(String.class)) {

				Predicate predicate = from.get(attribute);
				if (exampleAccessor.isIgnoreCaseForPath(currentPath)) {

					predicate = lower(alias, predicate);
					attributeValue = attributeValue.toString().toLowerCase();
				}

				switch (exampleAccessor.getStringMatcherForPath(currentPath)) {

					case DEFAULT:
					case EXACT:
						Predicate equal = equal(alias, predicate, attributeValue);
						predicates.add(equal);
						break;
					case CONTAINING:
						predicates.add(like(alias, //
								predicate, //
								"%" + escapeCharacter.escape(attributeValue.toString()) + "%" //
						));
						break;
					case STARTING:
						predicates.add(like(alias, //
								predicate, //
								escapeCharacter.escape(attributeValue.toString()) + "%" //
						));
						break;
					case ENDING:
						predicates.add(like(alias, //
								predicate, //
								"%" + escapeCharacter.escape(attributeValue.toString()) //
						));
						break;
					default:
						throw new IllegalArgumentException(
								"Unsupported StringMatcher " + exampleAccessor.getStringMatcherForPath(currentPath));
				}
			} else {
				Predicate equal = equal(alias, from.get(attribute), attributeValue);
				predicates.add(equal);
			}
		}

		return predicates;
	}

	private static Predicate isNull(String alias, Predicate predicate) {
		return predicate.attributesToClause(alias, " IS NULL");
	}

	private static Predicate equal(String alias, Predicate predicate, Object attributeValue) {

		if (attributeValue.getClass().equals(String.class)) {
			return predicate.attributesToClause(alias, " = '" + attributeValue + "'");
		}

		return predicate.attributesToClause(alias, " = " + attributeValue);
	}

	private static Predicate like(String alias, Predicate predicate, String expression) {

		String likeExpression = " like '" + expression + "'";

		if (predicate.clause.isEmpty()) {
			return predicate.attributesToClause(alias, likeExpression);
		} else {
			return new Predicate(predicate.clause + likeExpression, new ArrayList<>(), predicate.joins);
		}
	}

	private static Predicate lower(String alias, Predicate predicate) {
		return predicate.attributesToClause("lower(" + alias, ")");
	}

	private static boolean isAssociation(Attribute<?, ?> attribute) {
		return ASSOCIATION_TYPES.contains(attribute.getPersistentAttributeType());
	}

	/**
	 * {@link PathNode} is used to dynamically grow a directed graph structure that allows to detect cycles within its
	 * direct predecessor nodes by comparing parent node values using {@link System#identityHashCode(Object)}.
	 *
	 * @author Christoph Strobl
	 */
	private static class PathNode {

		String name;
		@Nullable PathNode parent;
		List<PathNode> siblings = new ArrayList<>();
		@Nullable Object value;

		PathNode(String edge, @Nullable PathNode parent, @Nullable Object value) {

			this.name = edge;
			this.parent = parent;
			this.value = value;
		}

		PathNode add(String attribute, @Nullable Object value) {

			PathNode node = new PathNode(attribute, this, value);
			siblings.add(node);
			return node;
		}

		boolean spansCycle() {

			if (value == null) {
				return false;
			}

			String identityHex = ObjectUtils.getIdentityHexString(value);
			PathNode current = parent;

			while (current != null) {

				if (current.value != null && ObjectUtils.getIdentityHexString(current.value).equals(identityHex)) {
					return true;
				}
				current = current.parent;
			}

			return false;
		}

		@Override
		public String toString() {

			StringBuilder sb = new StringBuilder();
			if (parent != null) {
				sb.append(parent);
				sb.append(" -");
				sb.append(name);
				sb.append("-> ");
			}

			sb.append("[{ ");
			sb.append(ObjectUtils.nullSafeToString(value));
			sb.append(" }]");
			return sb.toString();
		}
	}
}
