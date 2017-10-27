/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.junit;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

/**
 * ClassRule: Starts scassandra before the tests run and stops scassandra when all tests have finished.
 *
 * Rule: Clears all primes and recorded activity between each test.
 */
public class ScassandraServerRule implements TestRule {

    private Scassandra scassandra;
    private boolean started = false;

    public ScassandraServerRule(){
        scassandra = ScassandraFactory.createServer();
    }

    public ScassandraServerRule(int binaryPort, int adminPort){
        scassandra = ScassandraFactory.createServer(binaryPort, adminPort);
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                if (!started) {
                    started = true;
                    scassandra.start();
                    try {
                        base.evaluate();
                    } finally {
                        scassandra.stop();
                        started = false;
                    }
                } else {
                    primingClient().clearAllPrimes();
                    activityClient().clearAllRecordedActivity();
                    base.evaluate();
                }
            }
        };
    }

    public PrimingClient primingClient() {
        return scassandra.primingClient();
    }

    public ActivityClient activityClient() {
        return scassandra.activityClient();
    }
}
