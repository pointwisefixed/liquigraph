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
package org.liquigraph.model.predicates;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import java.util.Collection;

public class ExecutionContextsMatchAnyContext implements Predicate<String> {
    private final Optional<Collection<String>> contexts;

    private ExecutionContextsMatchAnyContext(Optional<Collection<String>> contexts) {
        this.contexts = contexts;
    }

    public static Predicate<String> BY_ANY_CONTEXT(Optional<Collection<String>> contexts) {
        return new ExecutionContextsMatchAnyContext(contexts);
    }

    @Override
    public boolean apply(String input) {
        return !contexts.isPresent() || contexts.get().contains(input);
    }
}
