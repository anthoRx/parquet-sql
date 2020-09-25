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

package io.github.anthorx.parquet.sql.model;

import java.util.ArrayList;
import java.util.List;

public class SQLRow {

    List<SQLField> fields = new ArrayList<>();

    public SQLRow() {
    }

    public <T> void addField(SQLField field) {
        fields.add(field);
    }

    public <T> void addField(String name, Object value) {
        fields.add(new SQLField(name, value));
    }

    public List<SQLField> getFields() {
        return fields;
    }


    public SQLField getField(int index) {
        return fields.get(index);
    }

    @Override
    public String toString() {
        return "Row{" +
                "fields=" + fields +
                '}';
    }
}
