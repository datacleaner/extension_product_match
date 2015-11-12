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
import java.util.List;
import java.util.Map;

import org.apache.metamodel.elasticsearch.ElasticSearchDataContext;
import org.apache.metamodel.util.HasName;
import org.datacleaner.api.Categorized;
import org.datacleaner.api.Configured;
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
@org.datacleaner.api.Description(value = "Identify the various commerial products by their name or by GTIN-13 code")
@Categorized(superCategory = ImproveSuperCategory.class, value = ReferenceDataCategory.class)
public class ProductMatchTransformer implements Transformer {

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

        for (ProductFieldTypes fieldType : OUTPUT_FIELD_TYPES) {
            columnNames.add(fieldType.getName());
        }

        return new OutputColumns(String.class, columnNames.toArray(new String[columnNames.size()]));
    }

    @Override
    public Object[] transform(InputRow row) {
        try (UpdateableDatastoreConnection connection = datastore.openConnection()) {
            final ElasticSearchDataContext dataContext = (ElasticSearchDataContext) connection.getDataContext();
            final Client client = dataContext.getElasticSearchClient();

            return transform(row, client);
        }
    }

    protected Object[] transform(InputRow row, Client client) {
        final Object[] result = new Object[OUTPUT_FIELD_TYPES.length];

        // TODO: Take care of mapping

        final String query = toString(row.getValue(inputColumns[0]));
        if (query == null) {
            // TODO: Return some "skipped" record
            return result;
        }

        final SearchResponse searchResult = client.prepareSearch("pod").setTypes("product")
                .setSearchType(SearchType.QUERY_AND_FETCH).setQuery(QueryBuilders.matchQuery("_all", query)).setSize(1)
                .execute().actionGet();

        final SearchHits hits = searchResult.getHits();
        if (hits.getTotalHits() == 0) {
            // TODO: Return a "non match" record
            return result;
        }

        final SearchHit hit = hits.getAt(0);

        final Map<String, Object> map = hit.sourceAsMap();
        for (int i = 0; i < OUTPUT_FIELD_TYPES.length; i++) {
            final String fieldName = OUTPUT_FIELD_TYPES[i].getFieldName();
            final Object value = map.get(fieldName);
            result[i] = value;
        }
        return result;
    }

    private String toString(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

}
