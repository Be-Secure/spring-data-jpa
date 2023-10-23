/*
 * Copyright 2008-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.metamodel.Metamodel;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.sample.User;
import org.springframework.data.jpa.provider.QueryExtractor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryRewriter;
import org.springframework.data.jpa.repository.sample.UserRepository;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Unit test for {@link SimpleJpaQuery}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Jens Schauder
 * @author Tom Hombergs
 * @author Mark Paluch
 * @author Greg Turnquist
 * @author Krzysztof Krason
 * @author Erik Pellizzon
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SimpleJpaQueryUnitTests {

	private static final String USER_QUERY = "select u from User u";
	private static final SpelExpressionParser PARSER = new SpelExpressionParser();
	private static final QueryMethodEvaluationContextProvider EVALUATION_CONTEXT_PROVIDER = QueryMethodEvaluationContextProvider.DEFAULT;

	private JpaQueryMethod method;

	@Mock EntityManager em;
	@Mock EntityManagerFactory emf;
	@Mock QueryExtractor extractor;
	@Mock jakarta.persistence.Query query;
	@Mock TypedQuery<Long> typedQuery;
	@Mock RepositoryMetadata metadata;
	@Mock ParameterBinder binder;
	@Mock Metamodel metamodel;

	private ProjectionFactory factory = new SpelAwareProxyProjectionFactory();

	@BeforeEach
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void setUp() throws SecurityException, NoSuchMethodException {

		when(em.getMetamodel()).thenReturn(metamodel);
		when(em.createQuery(anyString())).thenReturn(query);
		when(em.createQuery(anyString(), eq(Long.class))).thenReturn(typedQuery);
		when(em.getEntityManagerFactory()).thenReturn(emf);
		when(em.getDelegate()).thenReturn(em);
		when(emf.createEntityManager()).thenReturn(em);
		when(metadata.getDomainType()).thenReturn((Class) User.class);
		when(metadata.getDomainTypeInformation()).thenReturn((TypeInformation) TypeInformation.of(User.class));
		when(metadata.getReturnedDomainClass(Mockito.any(Method.class))).thenReturn((Class) User.class);
		when(metadata.getReturnType(Mockito.any(Method.class)))
				.thenAnswer(invocation -> TypeInformation.fromReturnTypeOf(invocation.getArgument(0)));

		Method setUp = UserRepository.class.getMethod("findByLastname", String.class);
		method = new JpaQueryMethod(setUp, metadata, factory, extractor);
	}

	@Test
	void prefersDeclaredCountQueryOverCreatingOne() throws Exception {

		method = new JpaQueryMethod(SampleRepository.class.getMethod("findAllWithExpressionInCountQuery", Pageable.class),
				metadata, factory, extractor);

		AnnotationBasedQueryContext jpaQuery = new AnnotationBasedQueryContext(method, em, "select u from User u", null,
				EVALUATION_CONTEXT_PROVIDER, PARSER, method.isNativeQuery(), QueryRewriter.IdentityQueryRewriter.INSTANCE);

		assertThat(jpaQuery.createCountQuery(
				new JpaParametersParameterAccessor(method.getParameters(), new Object[] { PageRequest.of(1, 10) })))
				.isEqualTo(typedQuery);
	}

	@Test // DATAJPA-77
	void doesNotApplyPaginationToCountQuery() throws Exception {

		when(em.createQuery(Mockito.anyString())).thenReturn(query);

		Method method = UserRepository.class.getMethod("findAllPaged", Pageable.class);
		JpaQueryMethod queryMethod = new JpaQueryMethod(method, metadata, factory, extractor);

		AbstractJpaQuery jpaQuery = new SimpleJpaQuery(queryMethod, em, "select u from User u", null,
				QueryRewriter.IdentityQueryRewriter.INSTANCE, EVALUATION_CONTEXT_PROVIDER, PARSER);
		jpaQuery.createCountQuery(
				new JpaParametersParameterAccessor(queryMethod.getParameters(), new Object[] { PageRequest.of(1, 10) }));

		verify(query, times(0)).setFirstResult(anyInt());
		verify(query, times(0)).setMaxResults(anyInt());
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void discoversNativeQuery() throws Exception {

		Method method = SampleRepository.class.getMethod("findNativeByLastname", String.class);
		JpaQueryMethod queryMethod = new JpaQueryMethod(method, metadata, factory, extractor);
		AnnotationBasedQueryContext jpaQuery = JpaQueryFactory.INSTANCE.fromMethodWithQueryString(queryMethod, em,
				queryMethod.getAnnotatedQuery(), null, QueryRewriter.IdentityQueryRewriter.INSTANCE,
				EVALUATION_CONTEXT_PROVIDER);

		assertThat(jpaQuery.isNativeQuery()).isTrue();

		when(em.createNativeQuery(anyString(), eq(User.class))).thenReturn(query);
		when(metadata.getReturnedDomainClass(method)).thenReturn((Class) User.class);

		jpaQuery.turnIntoJpaQuery(jpaQuery.getQueryString(),
				new JpaParametersParameterAccessor(queryMethod.getParameters(), new Object[] { "Matthews" }));

		verify(em).createNativeQuery("SELECT u FROM User u WHERE u.lastname = ?1", User.class);
	}

	@Test // DATAJPA-554
	void rejectsNativeQueryWithDynamicSort() throws Exception {

		Method method = SampleRepository.class.getMethod("findNativeByLastname", String.class, Sort.class);
		assertThatExceptionOfType(InvalidJpaQueryMethodException.class).isThrownBy(() -> createJpaQuery(method));
	}

	@Test // DATAJPA-352
	@SuppressWarnings("unchecked")
	void doesNotValidateCountQueryIfNotPagingMethod() throws Exception {

		Method method = SampleRepository.class.getMethod("findByAnnotatedQuery");
		when(em.createQuery(Mockito.contains("count"))).thenThrow(IllegalArgumentException.class);

		createJpaQuery(method);
	}

	@Test
	void createsASimpleJpaQueryFromAnnotation() throws Exception {

		AnnotationBasedQueryContext query = createJpaQuery(SampleRepository.class.getMethod("findByAnnotatedQuery"));
		assertThat(query).isInstanceOf(AnnotationBasedQueryContext.class);
	}

	@Test
	void createsANativeJpaQueryFromAnnotation() throws Exception {

		AnnotationBasedQueryContext query = createJpaQuery(
				SampleRepository.class.getMethod("findNativeByLastname", String.class));

		assertThat(query.isNativeQuery()).isTrue();
	}

	@Test // DATAJPA-757
	void createsNativeCountQuery() throws Exception {

		when(em.createNativeQuery(anyString())).thenReturn(query);

		AnnotationBasedQueryContext jpaQuery = createJpaQuery(
				UserRepository.class.getMethod("findUsersInNativeQueryWithPagination", Pageable.class));

		jpaQuery.createCountQuery(new JpaParametersParameterAccessor(jpaQuery.getQueryMethod().getParameters(),
				new Object[] { PageRequest.of(0, 10) }));

		verify(em, times(3)).createNativeQuery(anyString());
	}

	@Test // DATAJPA-885
	void projectsWithManuallyDeclaredQuery() throws Exception {

		AnnotationBasedQueryContext jpaQuery = createJpaQuery(SampleRepository.class.getMethod("projectWithExplicitQuery"));

		jpaQuery.turnIntoJpaQuery(jpaQuery.getQueryString(),
				new JpaParametersParameterAccessor(jpaQuery.getQueryMethod().getParameters(), new Object[0]));

		// Two times, first one is from the query validation
		verify(em, times(2)).createQuery(anyString());
	}

	@Test // DATAJPA-1307
	void jdbcStyleParametersOnlyAllowedInNativeQueries() throws Exception {

		// just verifying that it doesn't throw an exception
		createJpaQuery(SampleRepository.class.getMethod("legalUseOfJdbcStyleParameters", String.class));

		Method illegalMethod = SampleRepository.class.getMethod("illegalUseOfJdbcStyleParameters", String.class);

		when(em.createQuery(contains(method.getCountQuery()))).thenThrow(new RuntimeException());

		assertThatIllegalArgumentException().isThrownBy(() -> {
			createJpaQuery(illegalMethod);
		});
	}

	@Test // DATAJPA-1163
	void resolvesExpressionInCountQuery() throws Exception {

		when(em.createQuery(Mockito.anyString())).thenReturn(query);

		Method method = SampleRepository.class.getMethod("findAllWithExpressionInCountQuery", Pageable.class);
		JpaQueryMethod queryMethod = new JpaQueryMethod(method, metadata, factory, extractor);

		AbstractJpaQuery jpaQuery = new SimpleJpaQuery(queryMethod, em, "select u from User u",
				"select count(u.id) from #{#entityName} u", QueryRewriter.IdentityQueryRewriter.INSTANCE,
				EVALUATION_CONTEXT_PROVIDER, PARSER);
		jpaQuery.createCountQuery(
				new JpaParametersParameterAccessor(queryMethod.getParameters(), new Object[] { PageRequest.of(1, 10) }));

		verify(em).createQuery(eq("select u from User u"));
		verify(em).createQuery(eq("select count(u.id) from User u"), eq(Long.class));
	}

	private AnnotationBasedQueryContext createJpaQuery(Method method) {

		JpaQueryMethod queryMethod = new JpaQueryMethod(method, metadata, factory, extractor);

		return JpaQueryFactory.INSTANCE.fromMethodWithQueryString(queryMethod, em, queryMethod.getAnnotatedQuery(),
				queryMethod.getCountQuery(), QueryRewriter.IdentityQueryRewriter.INSTANCE, EVALUATION_CONTEXT_PROVIDER);
	}

	interface SampleRepository {

		@Query(value = "SELECT u FROM User u WHERE u.lastname = ?1", nativeQuery = true)
		List<User> findNativeByLastname(String lastname);

		@Query(value = "SELECT u FROM User u WHERE u.lastname = ?1", nativeQuery = true)
		List<User> findNativeByLastname(String lastname, Sort sort);

		@Query(value = "SELECT u FROM User u WHERE u.lastname = ?1", nativeQuery = true)
		List<User> findNativeByLastname(String lastname, Pageable pageable);

		@Query(value = "SELECT u FROM User u WHERE u.lastname = ?", nativeQuery = true)
		List<User> legalUseOfJdbcStyleParameters(String lastname);

		@Query(value = "SELECT u FROM User u WHERE u.lastname = ?")
		List<User> illegalUseOfJdbcStyleParameters(String lastname);

		@Query(USER_QUERY)
		List<User> findByAnnotatedQuery();

		@Query(USER_QUERY)
		Page<User> pageByAnnotatedQuery(Pageable pageable);

		@Query("select u from User u")
		Collection<UserProjection> projectWithExplicitQuery();

		@Query(value = "select u from #{#entityName} u", countQuery = "select count(u.id) from #{#entityName} u")
		List<User> findAllWithExpressionInCountQuery(Pageable pageable);

	}

	interface UserProjection {}
}
