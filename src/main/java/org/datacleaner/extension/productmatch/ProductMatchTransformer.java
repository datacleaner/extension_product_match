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

    /**
     * Represents all fields in the search index
     */
    public static enum SearchField {
        // pseudo-fields
        SCORE, ALL,

        // product related
        GTIN_NM, GTIN_CD,

        // measures
        PKG_UNIT, M_G, M_OZ, M_ML, M_FLOZ,

        // brand related
        BRAND_NM, BRAND_LINK, BSIN,

        // category related
        GPC_SEGMENT, GPC_FAMILY, GPC_CLASS, GPC_BRICK,

        // contact related
        GLN_NM, GLN_ADDR_02, GLN_ADDR_03, GLN_ADDR_04, GLN_ADDR_POSTALCODE, GLN_ADDR_CITY, GLN_COUNTRY_ISO_CD;

        public String getFieldName() {
            if (this == SCORE) {
                throw new UnsupportedOperationException();
            }
            if (this == ALL) {
                return "_all";
            }
            return name();
        }

        public boolean isPseudoField() {
            return this == SCORE || this == ALL;
        }
    }

    public static enum InputField implements HasName {

        PRODUCT_DESCRIPTION_TEXT("Product description", SearchField.ALL),

        PRODUCT_NAME("Product name", SearchField.GTIN_NM),

        BRAND_NAME("Brand name", SearchField.BRAND_NM),

        GTIN_CODE("GTIN code", SearchField.GTIN_CD),

        BSIN_CODE("BSIN code", SearchField.BSIN),;

        private final String _name;
        private final SearchField _searchField;

        private InputField(String name, SearchField searchField) {
            _name = name;
            _searchField = searchField;
        }

        @Override
        public String getName() {
            return _name;
        }

        public SearchField getSearchField() {
            return _searchField;
        }
    }

    public static enum OutputField implements HasName {
        MATCH_STATUS("Match status", null),

        MATCH_SCORE("Match score", SearchField.SCORE, Number.class),

        GTIN_CODE("GTIN code", SearchField.GTIN_CD),

        PRODUCT_NAME("Product name", SearchField.GTIN_NM),

        BRAND_NAME("Brand name", SearchField.BRAND_NM),

        BSIN_CODE("BSIN code", SearchField.BSIN),

        GPC_SEGMENT("GPC segment", SearchField.GPC_SEGMENT),

        GPC_FAMILY("GPC family", SearchField.GPC_FAMILY), GPC_CLASS("GPC class", SearchField.GPC_CLASS),

        GPC_BRICK("GPC brick", SearchField.GPC_BRICK);

        private final String _name;
        private final SearchField _searchField;
        private final Class<?> _dataType;

        private OutputField(String name, SearchField searchField) {
            this(name, searchField, String.class);
        }

        private OutputField(String name, SearchField searchField, Class<?> dataType) {
            _name = name;
            _searchField = searchField;
            _dataType = dataType;
        }

        public Class<?> getDataType() {
            return _dataType;
        }

        @Override
        public String getName() {
            return _name;
        }

        public SearchField getSearchField() {
            return _searchField;
        }
    }

    @Configured(value = "Input")
    InputColumn<?>[] inputColumns;

    @Configured
    @MappedProperty("Input")
    InputField[] inputMapping;

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
        final OutputField[] outputFields = OutputField.values();
        final List<String> columnNames = new ArrayList<>(outputFields.length);
        final List<Class<?>> classes = new ArrayList<>(outputFields.length);

        for (OutputField fieldType : outputFields) {
            columnNames.add(fieldType.getName());
            classes.add(fieldType.getDataType());
        }

        return new OutputColumns(columnNames.toArray(new String[columnNames.size()]),
                classes.toArray(new Class[classes.size()]));
    }

    @Override
    public Object[] transform(InputRow row) {
        final Map<SearchField, Object> input = createInputMap(row);
        try (UpdateableDatastoreConnection connection = datastore.openConnection()) {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) connection.getDataContext();
            final Client client = dataContext.getElasticSearchClient();

            return transform(input, client);
        }
    }

    protected Object[] transform(Map<SearchField, Object> input, Client client) {
        final Object[] result = new Object[OutputField.values().length];

        // ensure that input=output, when no match is found
        applySearchHitToResult(input, result);

        if (input.isEmpty()) {
            result[0] = MATCH_STATUS_SKIPPED;
            return result;
        }

        final String gtinCode = normalizeGtinCode(input.get(SearchField.GTIN_CD));
        if (gtinCode != null) {
            // look up product based on GTIN code
            final SearchRequestBuilder lookup = client.prepareSearch("pod").setTypes("product")
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setQuery(QueryBuilders.termQuery(SearchField.GTIN_CD.getFieldName(), gtinCode));
            final Map<SearchField, Object> lookupResult = executeSearch(lookup);

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

        final List<QueryBuilder> queryBuilders = createQueryBuilders(input);

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

        final Map<SearchField, Object> matchResult = executeSearch(search);
        if (matchResult == null) {
            result[0] = MATCH_STATUS_NO_MATCH;
            return result;
        }

        result[0] = getMatchVerdict(input, matchResult);
        applySearchHitToResult(matchResult, result);
        return result;
    }

    private List<QueryBuilder> createQueryBuilders(Map<SearchField, Object> input) {
        final List<QueryBuilder> queryBuilders = new ArrayList<>();

        final MatchQueryBuilder productNameQuery = addMatchQueryBuilder(queryBuilders, input, SearchField.GTIN_NM);
        final MatchQueryBuilder brandNameQuery = addMatchQueryBuilder(queryBuilders, input, SearchField.BRAND_NM);
        final MatchQueryBuilder descriptionQuery = addMatchQueryBuilder(queryBuilders, input, SearchField.ALL, "_all");

        // some tweaks
        if (descriptionQuery != null) {
            // description is there
            if (productNameQuery != null) {
                // boost product name a bit
                productNameQuery.boost(2.5f);
            }
            if (brandNameQuery != null) {
                // boost brand name a bit
                brandNameQuery.boost(2);
            }
        }

        return queryBuilders;
    }

    private MatchQueryBuilder addMatchQueryBuilder(List<QueryBuilder> queryBuilders, Map<SearchField, Object> input,
            SearchField type) {
        return addMatchQueryBuilder(queryBuilders, input, type, type.getFieldName());
    }

    private MatchQueryBuilder addMatchQueryBuilder(List<QueryBuilder> queryBuilders, Map<SearchField, Object> input,
            SearchField type, String fieldName) {
        final Object value = input.get(type);
        if (value != null) {
            final MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery(fieldName, value);
            queryBuilders.add(queryBuilder);
            return queryBuilder;
        }
        return null;
    }

    private String getMatchVerdict(final Map<SearchField, Object> input, final Map<SearchField, Object> searchResult) {

        // TODO: compare input and search result
        return MATCH_STATUS_GOOD;
    }

    protected static String normalizeGtinCode(Object gtinObj) {
        if (gtinObj == null) {
            return null;
        }
        final String gtinStr = gtinObj.toString().trim().replace(" ", "").replace("-", "").replace("_", "");
        if (gtinStr.isEmpty()) {
            return null;
        }
        try {
            final long gtinNumber = Long.parseLong(gtinStr);
            return String.format("%013d", gtinNumber);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void applySearchHitToResult(Map<SearchField, Object> searchResult, Object[] result) {
        final OutputField[] outputFields = OutputField.values();
        for (int i = 0; i < outputFields.length; i++) {
            final SearchField searchField = outputFields[i].getSearchField();
            if (searchField != null) {
                final Object value = searchResult.get(searchField);
                result[i] = value;
            }
        }
    }

    private Map<SearchField, Object> executeSearch(SearchRequestBuilder search) {
        final SearchResponse searchResponse = search.setSize(1).execute().actionGet();

        final SearchHits hits = searchResponse.getHits();
        if (hits.getTotalHits() == 0) {
            return null;
        }

        final Map<SearchField, Object> searchResult = new EnumMap<>(SearchField.class);

        final SearchHit hit = hits.getAt(0);

        final float score = hit.getScore();
        searchResult.put(SearchField.SCORE, score);

        final Map<String, Object> sourceAsMap = hit.sourceAsMap();
        final SearchField[] searchFields = SearchField.values();
        for (int i = 0; i < searchFields.length; i++) {
            final SearchField searchField = searchFields[i];
            if (!searchField.isPseudoField()) {
                final Object value = sourceAsMap.get(searchField.getFieldName());
                searchResult.put(searchField, value);
            }
        }

        return searchResult;
    }

    private Map<SearchField, Object> createInputMap(InputRow row) {
        final Map<SearchField, Object> map = new EnumMap<>(SearchField.class);
        for (int i = 0; i < inputColumns.length; i++) {
            final Object value = row.getValue(inputColumns[i]);
            if (value instanceof String && ((String) value).trim().isEmpty()) {
                // skip blanks
                continue;
            }
            if (value != null) {
                final InputField fieldType = inputMapping[i];
                final SearchField searchField = fieldType.getSearchField();
                final Object existingValue = map.get(searchField);
                if (existingValue == null) {
                    map.put(searchField, value);
                } else {
                    // append the values if there are more that are mapped to
                    // the same field type
                    map.put(searchField, existingValue + " " + value);
                }
            }
        }
        return map;
    }
}
