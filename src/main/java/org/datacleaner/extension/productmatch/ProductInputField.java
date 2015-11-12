package org.datacleaner.extension.productmatch;

import org.apache.metamodel.util.HasName;

public enum ProductInputField implements HasName {

    PRODUCT_DESCRIPTION_TEXT("Product description", ProductSearchField.ALL),

    PRODUCT_NAME("Product name", ProductSearchField.GTIN_NM),

    BRAND_NAME("Brand name", ProductSearchField.BRAND_NM),

    GTIN_CODE("GTIN code", ProductSearchField.GTIN_CD),

    BSIN_CODE("BSIN code", ProductSearchField.BSIN),;

    private final String _name;
    private final ProductSearchField _searchField;

    private ProductInputField(String name, ProductSearchField searchField) {
        _name = name;
        _searchField = searchField;
    }

    @Override
    public String getName() {
        return _name;
    }

    public ProductSearchField getSearchField() {
        return _searchField;
    }
}