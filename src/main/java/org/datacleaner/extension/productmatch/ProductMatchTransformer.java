/**
 * DataCleaner (community edition)
 * Copyright (C) 2013 Human Inference
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */

package org.datacleaner.extension.productmatch;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.elasticsearch.ElasticSearchDataContext;
import org.apache.metamodel.util.HasName;
import org.datacleaner.api.Categorized;
import org.datacleaner.api.Configured;
import org.datacleaner.api.Description;
import org.datacleaner.api.Initialize;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.MappedProperty;
import org.datacleaner.api.OutputColumns;
import org.datacleaner.api.Transformer;
import org.datacleaner.components.categories.ImproveSuperCategory;
import org.datacleaner.components.categories.ReferenceDataCategory;
import org.datacleaner.connection.ElasticSearchDatastore;
import org.datacleaner.connection.ElasticSearchDatastore.ClientType;
import org.datacleaner.connection.UpdateableDatastoreConnection;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

@javax.inject.Named(value = "Product Matching")
@Description(value = "Match your product descriptions and codes with the Product Open Data (POD) database."
        + "The database contains product names, brand names as well as GTIN and BSIN codes for more than 900,000 products.\n"
        + "You can use this component either for validating your existing product information, appending information to it"
        + "or even for looking up the complete product information if you have a GTIN barcode.\n"
        + "The output column 'Match status' can contain either of the following values:\n" + "<ul>"
        + "<li>'GOOD_MATCH' - indicates a match with a good amount of certainty.</li>"
        + "<li>'POTENTIAL_MATCH' - A doubtful match which is potentially correct, but could also very well be incorrect.</li>"
        + "<li>'NO_MATCH' - No match at all or only very poor matches.</li>"
        + "<li>'SKIPPED' - The record was skipped - typically because there wasn't enough input.</li>" + "</ul>")
@Categorized(superCategory = ImproveSuperCategory.class, value = ReferenceDataCategory.class)
public class ProductMatchTransformer implements Transformer {

    public static final String MATCH_STATUS_GOOD = "GOOD_MATCH";
    public static final String MATCH_STATUS_POTENTIAL = "POTENTIAL_MATCH";
    public static final String MATCH_STATUS_NO_MATCH = "NO_MATCH";
    public static final String MATCH_STATUS_SKIPPED = "SKIPPED";

    public static enum ProductFieldTypes implements HasName {
        PRODUCT_DESCRIPTION_TEXT("Any product description (free form)", null),

        PRODUCT_NAME("Product name", "GTIN_NM"),

        BRAND_NAME("Brand name", "BRAND_NM"),

        GTIN_CODE("GTIN code", "GTIN_CD"),

        BSIN_CODE("BSIN code", "BSIN"),

        GPC_SEGMENT("GPC segment", "GPC_SEGMENT"),

        GPC_FAMILY("GPC family", "GPC_FAMILY"),

        GPC_CLASS("GPC class", "GPC_CLASS"),

        GPC_BRICK("GPC brick", "GPC_BRICK"),

        ;

        private final String _name;
        private final String _fieldName;

        private ProductFieldTypes(String name, String fieldName) {
            _name = name;
            _fieldName = fieldName;
        }

        @Override
        public String getName() {
            return _name;
        }

        public String getFieldName() {
            return _fieldName;
        }
    }

    private static final ProductFieldTypes[] OUTPUT_FIELD_TYPES = { ProductFieldTypes.GTIN_CODE,
            ProductFieldTypes.PRODUCT_NAME, ProductFieldTypes.BRAND_NAME, ProductFieldTypes.BSIN_CODE,
            ProductFieldTypes.GPC_SEGMENT, ProductFieldTypes.GPC_FAMILY, ProductFieldTypes.GPC_CLASS,
            ProductFieldTypes.GPC_BRICK };

    @Configured(value = "Input")
    InputColumn<?>[] inputColumns;

    @Configured
    @MappedProperty("Input")
    ProductFieldTypes[] inputMapping;

    ElasticSearchDatastore datastore;

    @Initialize
    public void init() {
        final String hostname = System.getProperty("org.datacleaner.extension.productmatch.hostname", "productvm");
        final String portString = System.getProperty("org.datacleaner.extension.productmatch.port", "9300");
        final int port = Integer.parseInt(portString);
        datastore = new ElasticSearchDatastore("pod", ClientType.TRANSPORT, hostname, port, "pod", "pod");
    }

    @Override
    public OutputColumns getOutputColumns() {
        final List<String> columnNames = new ArrayList<>();

        columnNames.add("Product match status");

        for (ProductFieldTypes fieldType : OUTPUT_FIELD_TYPES) {
            columnNames.add(fieldType.getName());
        }

        return new OutputColumns(String.class, columnNames.toArray(new String[columnNames.size()]));
    }

    @Override
    public Object[] transform(InputRow row) {
        final Map<ProductFieldTypes, String> input = createInputMap(row);
        try (UpdateableDatastoreConnection connection = datastore.openConnection()) {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) connection.getDataContext();
            final Client client = dataContext.getElasticSearchClient();

            return transform(input, client);
        }
    }

    protected Object[] transform(Map<ProductFieldTypes, String> input, Client client) {
        final Object[] result = new Object[1 + OUTPUT_FIELD_TYPES.length];

        // TODO: Take care of mapping

        final String query = input.get(ProductFieldTypes.PRODUCT_DESCRIPTION_TEXT);
        if (query == null) {
            result[0] = MATCH_STATUS_SKIPPED;
            return result;
        }

        final SearchResponse searchResult = client.prepareSearch("pod").setTypes("product")
                .setSearchType(SearchType.QUERY_AND_FETCH).setQuery(QueryBuilders.matchQuery("_all", query)).setSize(1)
                .execute().actionGet();

        final SearchHits hits = searchResult.getHits();
        if (hits.getTotalHits() == 0) {
            result[0] = MATCH_STATUS_NO_MATCH;
            return result;
        }

        final SearchHit hit = hits.getAt(0);
        result[0] = MATCH_STATUS_GOOD;

        final Map<String, Object> map = hit.sourceAsMap();
        for (int i = 0; i < OUTPUT_FIELD_TYPES.length; i++) {
            final String fieldName = OUTPUT_FIELD_TYPES[i].getFieldName();
            final Object value = map.get(fieldName);
            result[i + 1] = value;
        }
        return result;
    }

    private Map<ProductFieldTypes, String> createInputMap(InputRow row) {
        final Map<ProductFieldTypes, String> map = new EnumMap<>(ProductFieldTypes.class);
        for (int i = 0; i < inputColumns.length; i++) {
            final Object value = row.getValue(inputColumns[i]);
            final String str = toString(value);
            if (str != null) {
                final ProductFieldTypes fieldType = inputMapping[i];
                final String existingValue = map.get(fieldType);
                if (existingValue == null) {
                    map.put(fieldType, str);
                } else {
                    // append the values if there are more that are mapped to
                    // the same field type
                    map.put(fieldType, existingValue + " " + str);
                }
            }
        }
        return map;
    }

    private String toString(Object value) {
        if (value == null) {
            return null;
        }
        final String string = value.toString().trim();
        if (string.isEmpty()) {
            return null;
        }
        return string;
    }

}
