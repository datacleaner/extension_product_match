package org.datacleaner.extension.productmatch;

/**
 * Represents all fields in the search index
 */
enum ProductSearchField {
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