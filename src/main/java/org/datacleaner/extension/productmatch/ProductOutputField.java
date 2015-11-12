package org.datacleaner.extension.productmatch;

import org.apache.metamodel.util.HasName;

enum ProductOutputField implements HasName {
    
    MATCH_STATUS("Match status", null),

    MATCH_SCORE("Match score", ProductSearchField.SCORE, Number.class),

    GTIN_CODE("GTIN code", ProductSearchField.GTIN_CD),

    PRODUCT_NAME("Product name", ProductSearchField.GTIN_NM),

    BRAND_NAME("Brand name", ProductSearchField.BRAND_NM),

    BSIN_CODE("BSIN code", ProductSearchField.BSIN),

    GPC_SEGMENT("GPC segment", ProductSearchField.GPC_SEGMENT),

    GPC_FAMILY("GPC family", ProductSearchField.GPC_FAMILY), GPC_CLASS("GPC class", ProductSearchField.GPC_CLASS),

    GPC_BRICK("GPC brick", ProductSearchField.GPC_BRICK);

    private final String _name;
    private final ProductSearchField _searchField;
    private final Class<?> _dataType;

    private ProductOutputField(String name, ProductSearchField searchField) {
        this(name, searchField, String.class);
    }

    private ProductOutputField(String name, ProductSearchField searchField, Class<?> dataType) {
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

    public ProductSearchField getSearchField() {
        return _searchField;
    }
}