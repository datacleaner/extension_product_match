package org.datacleaner.extension.productmatch;

import static org.junit.Assert.assertEquals;

import org.datacleaner.api.InputColumn;
import org.datacleaner.data.MockInputColumn;
import org.datacleaner.data.MockInputRow;
import org.junit.Test;

import cern.colt.Arrays;

public class ProductMatchTransformerTest {

    @Test
    public void testPlainTextMatchCocaCola() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        MockInputColumn<String> column = new MockInputColumn<>("product");

        transformer.inputColumns = new InputColumn[] { column };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldTypes[] {ProductMatchTransformer.ProductFieldTypes.PRODUCT_DESCRIPTION_TEXT};
        
        transformer.init();
        
        Object[] result = transformer.transform(new MockInputRow().put(column, "Coca-cola"));
        
        assertEquals("[null, Coca Cola 2 litros||Refrigerantes | COCA COLA 2 LTRS, Coca-Cola, 7894900011517, 5MRM4M, Food/Beverage/Tobacco, null, null, null]", Arrays.toString(result));
    }
}
