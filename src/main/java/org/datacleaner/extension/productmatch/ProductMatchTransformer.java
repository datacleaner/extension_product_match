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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.metamodel.elasticsearch.ElasticSearchDataContext;
import org.datacleaner.api.Categorized;
import org.datacleaner.api.Configured;
import org.datacleaner.api.Description;
import org.datacleaner.api.HasAnalyzerResult;
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
public class ProductMatchTransformer implements Transformer, HasAnalyzerResult<ProductMatchResult> {
    
    private static final int INDEX_MATCH_STATUS = ProductOutputField.MATCH_STATUS.ordinal();
    private static final int INDEX_SEGMENT = ProductOutputField.GPC_SEGMENT.ordinal();

    public static final String MATCH_STATUS_GOOD = "GOOD_MATCH";
    public static final String MATCH_STATUS_POTENTIAL = "POTENTIAL_MATCH";
    public static final String MATCH_STATUS_NO_MATCH = "NO_MATCH";
    public static final String MATCH_STATUS_SKIPPED = "SKIPPED";

    @Configured(value = "Input")
    InputColumn<?>[] inputColumns;

    @Configured
    @MappedProperty("Input")
    ProductInputField[] inputMapping;

    private ElasticSearchDatastore datastore;
    private final ConcurrentHashMap<String, AtomicInteger> _matchStatuses = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> _segments = new ConcurrentHashMap<>();

    @Initialize
    public void init() {
        _matchStatuses.clear();
        _segments.clear();
        
        final String hostname = System.getProperty("org.datacleaner.extension.productmatch.hostname", "productvm");
        final String portString = System.getProperty("org.datacleaner.extension.productmatch.port", "9300");
        final int port = Integer.parseInt(portString);
        datastore = new ElasticSearchDatastore("pod", ClientType.TRANSPORT, hostname, port, "pod", "pod");
    }

    @Override
    public OutputColumns getOutputColumns() {
        final ProductOutputField[] outputFields = ProductOutputField.values();
        final List<String> columnNames = new ArrayList<>(outputFields.length);
        final List<Class<?>> classes = new ArrayList<>(outputFields.length);

        for (ProductOutputField fieldType : outputFields) {
            columnNames.add(fieldType.getName());
            classes.add(fieldType.getDataType());
        }

        return new OutputColumns(columnNames.toArray(new String[columnNames.size()]),
                classes.toArray(new Class[classes.size()]));
    }

    @Override
    public Object[] transform(InputRow row) {
        final Map<ProductSearchField, Object> input = createInputMap(row);
        final Object[] result;
        try (UpdateableDatastoreConnection connection = datastore.openConnection()) {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) connection.getDataContext();
            final Client client = dataContext.getElasticSearchClient();

            result = transform(input, client);
        }
        
        // update match status map
        {
            final String matchStatus = (String) result[INDEX_MATCH_STATUS];
            final AtomicInteger newCounter = new AtomicInteger(0);
            final AtomicInteger existingCounter = _matchStatuses.putIfAbsent(matchStatus, newCounter);
            final AtomicInteger counter = existingCounter == null? newCounter : existingCounter;
            counter.incrementAndGet();
        }
        
        // update segment status map (only if segment is found)
        {
            final String segment = (String) result[INDEX_SEGMENT];
            if (segment != null) {
                final AtomicInteger newCounter = new AtomicInteger(0);
                final AtomicInteger existingCounter = _segments.putIfAbsent(segment, newCounter);
                final AtomicInteger counter = existingCounter == null? newCounter : existingCounter;
                counter.incrementAndGet();
            }
        }
        
        return result;
    }

    protected Object[] transform(Map<ProductSearchField, Object> input, Client client) {
        final Object[] result = new Object[ProductOutputField.values().length];

        // ensure that input=output, when no match is found
        applySearchHitToResult(input, result);

        if (input.isEmpty()) {
            result[INDEX_MATCH_STATUS] = MATCH_STATUS_SKIPPED;
            return result;
        }

        final String gtinCode = normalizeGtinCode(input.get(ProductSearchField.GTIN_CD));
        if (gtinCode != null) {
            // look up product based on GTIN code
            final SearchRequestBuilder lookup = client.prepareSearch("pod").setTypes("product")
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setQuery(QueryBuilders.termQuery(ProductSearchField.GTIN_CD.getFieldName(), gtinCode));
            final Map<ProductSearchField, Object> lookupResult = executeSearch(lookup);

            if (lookupResult != null) {

                if (input.size() == 1) {
                    // this is a lookup-only scenario, everything is good now
                    // then
                    applySearchHitToResult(lookupResult, result);
                    result[INDEX_MATCH_STATUS] = MATCH_STATUS_GOOD;
                    return result;
                } else {
                    // some fields should be compared
                    final String matchVerdict = getMatchVerdict(input, lookupResult);
                    switch (matchVerdict) {
                    case MATCH_STATUS_GOOD:
                    case MATCH_STATUS_POTENTIAL:
                        // OK the lookup seems at least potential - we'll return
                        // this
                        applySearchHitToResult(lookupResult, result);
                        result[INDEX_MATCH_STATUS] = matchVerdict;
                        return result;
                    }
                }
            }else{
                if (input.size() == 1){
                    result[INDEX_MATCH_STATUS] = MATCH_STATUS_NO_MATCH;
                    return result;
                }
            }
        }

        final List<QueryBuilder> queryBuilders = createQueryBuilders(input);

        if (queryBuilders.isEmpty()) {
            result[INDEX_MATCH_STATUS] = MATCH_STATUS_SKIPPED;
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

        final Map<ProductSearchField, Object> matchResult = executeSearch(search);
        if (matchResult == null) {
            result[INDEX_MATCH_STATUS] = MATCH_STATUS_NO_MATCH;
            return result;
        }

        final String matchVerdict = getMatchVerdict(input, matchResult);
        result[INDEX_MATCH_STATUS] = matchVerdict;
        switch (matchVerdict) {
        case MATCH_STATUS_GOOD:
        case MATCH_STATUS_POTENTIAL:
            // only proper matches causes update to result
            applySearchHitToResult(matchResult, result);
        }
        return result;
    }

    private List<QueryBuilder> createQueryBuilders(Map<ProductSearchField, Object> input) {
        final List<QueryBuilder> queryBuilders = new ArrayList<>();

        final MatchQueryBuilder productNameQuery = addMatchQueryBuilder(queryBuilders, input,
                ProductSearchField.GTIN_NM);
        final MatchQueryBuilder brandNameQuery = addMatchQueryBuilder(queryBuilders, input,
                ProductSearchField.BRAND_NM);
        final MatchQueryBuilder descriptionQuery = addMatchQueryBuilder(queryBuilders, input, ProductSearchField.ALL,
                "_all");

        if (descriptionQuery != null) {
            // description is there

            if (productNameQuery == null) {
                // also apply description to "product name"
                addMatchQueryBuilder(queryBuilders, input, ProductSearchField.ALL,
                        ProductSearchField.GTIN_NM.getFieldName());
            }

            if (brandNameQuery == null) {
                // also apply description to "brand name"
                addMatchQueryBuilder(queryBuilders, input, ProductSearchField.ALL,
                        ProductSearchField.BRAND_NM.getFieldName());
            }
        }

        return queryBuilders;
    }

    private MatchQueryBuilder addMatchQueryBuilder(List<QueryBuilder> queryBuilders,
            Map<ProductSearchField, Object> input, ProductSearchField type) {
        return addMatchQueryBuilder(queryBuilders, input, type, type.getFieldName());
    }

    private MatchQueryBuilder addMatchQueryBuilder(List<QueryBuilder> queryBuilders,
            Map<ProductSearchField, Object> input, ProductSearchField type, String fieldName) {
        final Object value = input.get(type);
        if (value != null) {
            final MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery(fieldName, value);
            queryBuilders.add(queryBuilder);
            return queryBuilder;
        }
        return null;
    }

    private String getMatchVerdict(final Map<ProductSearchField, Object> input,
            final Map<ProductSearchField, Object> searchResult) {
        final Number score = (Number) searchResult.get(ProductSearchField.SCORE);
        if (score != null) {
            if (score.floatValue() < 3f) {
                return MATCH_STATUS_NO_MATCH;
            }
            if (score.floatValue() < 7f) {
                return MATCH_STATUS_POTENTIAL;
            }
        }

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

    private void applySearchHitToResult(Map<ProductSearchField, Object> searchResult, Object[] result) {
        final ProductOutputField[] outputFields = ProductOutputField.values();
        for (int i = 0; i < outputFields.length; i++) {
            final ProductSearchField searchField = outputFields[i].getSearchField();
            if (searchField != null) {
                final Object value = searchResult.get(searchField);
                result[i] = value;
            }
        }
    }

    private Map<ProductSearchField, Object> executeSearch(SearchRequestBuilder search) {
        final SearchResponse searchResponse = search.setSize(1).execute().actionGet();

        final SearchHits hits = searchResponse.getHits();
        if (hits.getTotalHits() == 0) {
            return null;
        }

        final Map<ProductSearchField, Object> searchResult = new EnumMap<>(ProductSearchField.class);

        final SearchHit hit = hits.getAt(0);

        final float score = hit.getScore();
        searchResult.put(ProductSearchField.SCORE, score);

        final Map<String, Object> sourceAsMap = hit.sourceAsMap();
        final ProductSearchField[] searchFields = ProductSearchField.values();
        for (int i = 0; i < searchFields.length; i++) {
            final ProductSearchField searchField = searchFields[i];
            if (!searchField.isPseudoField()) {
                final Object value = sourceAsMap.get(searchField.getFieldName());
                searchResult.put(searchField, value);
            }
        }

        return searchResult;
    }

    private Map<ProductSearchField, Object> createInputMap(InputRow row) {
        final Map<ProductSearchField, Object> map = new EnumMap<>(ProductSearchField.class);
        for (int i = 0; i < inputColumns.length; i++) {
            final Object value = row.getValue(inputColumns[i]);
            if (value instanceof String && ((String) value).trim().isEmpty()) {
                // skip blanks
                continue;
            }
            if (value != null) {
                final ProductInputField fieldType = inputMapping[i];
                final ProductSearchField searchField = fieldType.getSearchField();
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

    @Override
    public ProductMatchResult getResult() {
        return new ProductMatchResult(_matchStatuses, _segments);
    }
}
