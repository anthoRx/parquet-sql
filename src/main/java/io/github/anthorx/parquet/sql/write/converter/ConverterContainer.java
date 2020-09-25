/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.github.anthorx.parquet.sql.write.converter;

import io.github.anthorx.parquet.sql.write.converter.types.*;

import java.util.*;

public class ConverterContainer {

    private Deque<ParquetSQLConverter> container;

    public ConverterContainer() {
        container = new LinkedList<>();
        registerDefaultConverters();
    }

    private void registerDefaultConverters() {
        container.add(new BigDecimalConverter());
        container.add(new DoubleConverter());
        container.add(new StringConverter());
        container.add(new TimestampConverter());
    }

    public void registerConverter(ParquetSQLConverter converter) {
        container.addFirst(converter);
    }

    /**
     * @return the last registered converter matching the given classFullName
     */
    public Optional<ParquetSQLConverter> getConverter(String classFullName) throws ClassNotFoundException {
        return getConverter(Class.forName(classFullName));
    }

    /**
     * @return the last registered converter matching the given clazz
     */
    public Optional<ParquetSQLConverter> getConverter(Class<?> clazz) {
        return container
                .stream()
                .filter(converter -> converter.accept(clazz))
                .findFirst();
    }
}
