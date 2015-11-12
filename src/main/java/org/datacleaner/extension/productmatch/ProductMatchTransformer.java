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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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

    public static enum ProductFieldType implements HasName {

        PRODUCT_DESCRIPTION_TEXT("Product description", null),

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

        private ProductFieldType(String name, String fieldName) {
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

    private static final ProductFieldType[] OUTPUT_FIELD_TYPES = { ProductFieldType.GTIN_CODE,
            ProductFieldType.PRODUCT_NAME, ProductFieldType.BRAND_NAME, ProductFieldType.BSIN_CODE,
            ProductFieldType.GPC_SEGMENT, ProductFieldType.GPC_FAMILY, ProductFieldType.GPC_CLASS,
            ProductFieldType.GPC_BRICK };

    @Configured(value = "Input")
    InputColumn<?>[] inputColumns;

    @Configured
    @MappedProperty("Input")
    ProductFieldType[] inputMapping;

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

        for (ProductFieldType fieldType : OUTPUT_FIELD_TYPES) {
            columnNames.add(fieldType.getName());
        }

        return new OutputColumns(String.class, columnNames.toArray(new String[columnNames.size()]));
    }

    @Override
    public Object[] transform(InputRow row) {
        final Map<ProductFieldType, String> input = createInputMap(row);
        try (UpdateableDatastoreConnection connection = datastore.openConnection()) {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) connection.getDataContext();
            final Client client = dataContext.getElasticSearchClient();

            return transform(input, client);
        }
    }

    protected Object[] transform(Map<ProductFieldType, String> input, Client client) {
        final Object[] result = new Object[1 + OUTPUT_FIELD_TYPES.length];

        // ensure that input=output, when no match is found
        applySearchHitToResult(input, result);

        if (input.isEmpty()) {
            result[0] = MATCH_STATUS_SKIPPED;
            return result;
        }

        final String gtinCode = normalizeGtinCode(input.get(ProductFieldType.GTIN_CODE));
        if (gtinCode != null) {
            // look up product based on GTIN code
            final SearchRequestBuilder lookup = client.prepareSearch("pod").setTypes("product")
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setQuery(QueryBuilders.termQuery(ProductFieldType.GTIN_CODE.getFieldName(), gtinCode));
            final Map<ProductFieldType, String> lookupResult = executeSearch(lookup);

            if (lookupResult != null) {

                if (input.size() == 1) {
                    // this is a lookup-only scenario, everything is good now
                    // then
                    applySearchHitToResult(lookupResult, result);
                    result[0] = MATCH_STATUS_GOOD;
                    return result;
                } else {
                    // some fields should be compared
                    final String matchResult = getMatchVerdict(input, lookupResult);
                    switch (matchResult) {
                    case MATCH_STATUS_GOOD:
                    case MATCH_STATUS_POTENTIAL:
                        // OK the lookup seems at least potential - we'll return
                        // this
                        applySearchHitToResult(lookupResult, result);
                        result[0] = matchResult;
                        return result;
                    }
                }
            }
        }

        final List<QueryBuilder> queryBuilders = new ArrayList<>();
        addQueryBuilder(queryBuilders, input, ProductFieldType.PRODUCT_NAME);
        addQueryBuilder(queryBuilders, input, ProductFieldType.BRAND_NAME);
        addQueryBuilder(queryBuilders, input, ProductFieldType.PRODUCT_DESCRIPTION_TEXT, "_all");

        if (queryBuilders.isEmpty()) {
            result[0] = MATCH_STATUS_SKIPPED;
            return result;
        }

        final QueryBuilder finalQueryBuilder;
        if (queryBuilders.size() == 1) {
            finalQueryBuilder = queryBuilders.get(0);
        } else {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (QueryBuilder childQueryBuilder : queryBuilders) {
                boolQuery = boolQuery.should(childQueryBuilder);
            }
            finalQueryBuilder = boolQuery;
        }

        final SearchRequestBuilder search = client.prepareSearch("pod").setTypes("product")
                .setSearchType(SearchType.QUERY_AND_FETCH).setQuery(finalQueryBuilder);

        final Map<ProductFieldType, String> matchResult = executeSearch(search);
        if (matchResult == null) {
            result[0] = MATCH_STATUS_NO_MATCH;
            return result;
        }

        result[0] = getMatchVerdict(input, matchResult);
        applySearchHitToResult(matchResult, result);
        return result;
    }

    private void addQueryBuilder(List<QueryBuilder> queryBuilders, Map<ProductFieldType, String> input,
            ProductFieldType type) {
        addQueryBuilder(queryBuilders, input, type, type.getFieldName());
    }

    private void addQueryBuilder(List<QueryBuilder> queryBuilders, Map<ProductFieldType, String> input,
            ProductFieldType type, String fieldName) {
        final String description = input.get(type);
        if (description != null) {
            final MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery(fieldName, description);
            queryBuilders.add(queryBuilder);
        }
    }

    private String getMatchVerdict(final Map<ProductFieldType, String> input,
            final Map<ProductFieldType, String> searchResult) {

        // TODO: compare input and search result
        return MATCH_STATUS_GOOD;
    }

    protected static String normalizeGtinCode(String gtin) {
        if (gtin == null) {
            return null;
        }
        gtin = gtin.trim().replace(" ", "").replace("-", "").replace("_", "");
        if (gtin.isEmpty()) {
            return null;
        }
        try {
            final long gtinNumber = Long.parseLong(gtin);
            return String.format("%013d", gtinNumber);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void applySearchHitToResult(Map<ProductFieldType, String> searchResult, Object[] result) {
        for (int i = 0; i < OUTPUT_FIELD_TYPES.length; i++) {
            final ProductFieldType productFieldType = OUTPUT_FIELD_TYPES[i];
            final Object value = searchResult.get(productFieldType);
            result[i + 1] = value;
        }
    }

    private Map<ProductFieldType, String> executeSearch(SearchRequestBuilder search) {
        final SearchResponse searchResponse = search.setSize(1).execute().actionGet();

        final SearchHits hits = searchResponse.getHits();
        if (hits.getTotalHits() == 0) {
            return null;
        }

        final SearchHit hit = hits.getAt(0);

        final Map<String, Object> sourceAsMap = hit.sourceAsMap();
        final Map<ProductFieldType, String> searchResult = new EnumMap<>(ProductFieldType.class);
        for (int i = 0; i < OUTPUT_FIELD_TYPES.length; i++) {
            final ProductFieldType productFieldType = OUTPUT_FIELD_TYPES[i];
            final Object value = sourceAsMap.get(productFieldType.getFieldName());
            searchResult.put(productFieldType, toString(value));
        }

        return searchResult;
    }

    private Map<ProductFieldType, String> createInputMap(InputRow row) {
        final Map<ProductFieldType, String> map = new EnumMap<>(ProductFieldType.class);
        for (int i = 0; i < inputColumns.length; i++) {
            final Object value = row.getValue(inputColumns[i]);
            final String str = toString(value);
            if (str != null) {
                final ProductFieldType fieldType = inputMapping[i];
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
