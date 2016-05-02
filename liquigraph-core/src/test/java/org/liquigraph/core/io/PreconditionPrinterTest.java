/**
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.liquigraph.core.io;

import com.google.common.base.Joiner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.liquigraph.connector.io.PreconditionPrinter;
import org.liquigraph.model.AndQuery;
import org.liquigraph.model.CompoundQuery;
import org.liquigraph.model.OrQuery;
import org.liquigraph.model.Precondition;
import org.liquigraph.model.PreconditionErrorPolicy;
import org.liquigraph.model.PreconditionQuery;
import org.liquigraph.model.SimpleQuery;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

public class PreconditionPrinterTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private PreconditionPrinter preconditionPrinter = new PreconditionPrinter();

    @Test
    public void prints_nothing_for_null_precondition_query() {
        assertThat(preconditionPrinter.print(null)).isEmpty();
    }

    @Test
    public void fails_to_print_when_given_an_unknown_query_type() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unsupported query type <org.liquigraph.core.io.PreconditionPrinterTest$1>");

        Precondition precondition = new Precondition();
        precondition.setQuery(new PreconditionQuery() {});
        preconditionPrinter.print(precondition);
    }

    @Test
    public void prints_simple_precondition_query() {
        Precondition precondition = precondition(
            simpleQuery("FROM a RETURN a"),
            PreconditionErrorPolicy.CONTINUE
        );

        String contents = Joiner.on("\n").join(preconditionPrinter.print(precondition));

        assertThat(contents).isEqualTo(
            "//Liquigraph precondition[if-not-met: CONTINUE]\n" +
            "FROM a RETURN a"
        );
    }

    @Test
    public void prints_compound_query() {
        Precondition precondition = precondition(
            orQuery(
                simpleQuery("FROM a RETURN a"),
                andQuery(
                    simpleQuery("FROM b RETURN b"),
                    simpleQuery("FROM c RETURN c")
                )
            ),
            PreconditionErrorPolicy.FAIL
        );

        String contents = Joiner.on("\n").join(preconditionPrinter.print(precondition));

        assertThat(contents).isEqualTo(
            "//Liquigraph precondition[if-not-met: FAIL]\n" +
            "((FROM a RETURN a) OR (((FROM b RETURN b) AND (FROM c RETURN c))))");
    }

    private Precondition precondition(PreconditionQuery query, PreconditionErrorPolicy aContinue) {
        Precondition precondition = new Precondition();
        precondition.setQuery(query);
        precondition.setPolicy(aContinue);
        return precondition;
    }

    private CompoundQuery orQuery(PreconditionQuery query1, PreconditionQuery query2) {
        OrQuery orQuery = new OrQuery();
        orQuery.setPreconditionQueries(newArrayList(query1, query2));
        return orQuery;
    }

    private CompoundQuery andQuery(PreconditionQuery query1, PreconditionQuery query2) {
        AndQuery andQuery = new AndQuery();
        andQuery.setPreconditionQueries(newArrayList(query1, query2));
        return andQuery;
    }

    private SimpleQuery simpleQuery(String query) {
        SimpleQuery simpleQuery = new SimpleQuery();
        simpleQuery.setQuery(query);
        return simpleQuery;
    }
}
