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
        assertEquals("[7894900011517, Coca Cola 2 litros||Refrigerantes | COCA COLA 2 LTRS, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]", Arrays.toString(result));
        
        result = transformer.transform(new MockInputRow().put(column, "Coca cola zero 1 liter"));
        assertEquals("[7894900701753, COCA COLA ZERO 1,, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]", Arrays.toString(result));
    }
    

    @Test
    public void testNoMatch() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        MockInputColumn<String> column = new MockInputColumn<>("product");

        transformer.inputColumns = new InputColumn[] { column };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldTypes[] {ProductMatchTransformer.ProductFieldTypes.PRODUCT_DESCRIPTION_TEXT};
        
        transformer.init();
        
        Object[] result = transformer.transform(new MockInputRow().put(column, "helloworldabracadabra"));
        assertEquals("[null, null, null, null, null, null, null, null]", Arrays.toString(result));
    }
}
