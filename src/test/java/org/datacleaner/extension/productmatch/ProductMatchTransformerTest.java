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
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_DESCRIPTION_TEXT };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(column, "Coca-cola"));
        assertEquals(
                "[GOOD_MATCH, 7894900011517, Coca Cola 2 litros||Refrigerantes | COCA COLA 2 LTRS, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]",
                Arrays.toString(result));

        result = transformer.transform(new MockInputRow().put(column, "Coca cola zero 1 liter"));
        assertEquals(
                "[GOOD_MATCH, 7894900701753, COCA COLA ZERO 1,, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]",
                Arrays.toString(result));
    }

    @Test
    public void testNoMatchUnknownTerms() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        MockInputColumn<String> column = new MockInputColumn<>("product");

        transformer.inputColumns = new InputColumn[] { column };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_DESCRIPTION_TEXT };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(column, "helloworldabracadabra"));
        assertEquals("[NO_MATCH, null, null, null, null, null, null, null, null]", Arrays.toString(result));
    }

    @Test
    public void testNoMatchNoQuery() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        MockInputColumn<String> column = new MockInputColumn<>("product");

        transformer.inputColumns = new InputColumn[] { column };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_DESCRIPTION_TEXT };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(column, ""));
        assertEquals("[SKIPPED, null, null, null, null, null, null, null, null]", Arrays.toString(result));

        result = transformer.transform(new MockInputRow().put(column, null));
        assertEquals("[SKIPPED, null, null, null, null, null, null, null, null]", Arrays.toString(result));
    }

    @Test
    public void testGtinLookup() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        MockInputColumn<String> column = new MockInputColumn<>("product");

        transformer.inputColumns = new InputColumn[] { column };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.GTIN_CODE };

        transformer.init();
        Object[] result = transformer.transform(new MockInputRow().put(column, "0300743288131"));
        assertEquals(
                "[GOOD_MATCH, 0300743288131, 1 Er Tablets 1x100 Mfg. Abbott Laboratories 240 mg,1 count, Abbott Laboratories, JLI2V7, Healthcare, null, null, null]",
                Arrays.toString(result));

        result = transformer.transform(new MockInputRow().put(column, "300743288131"));
        assertEquals(
                "[GOOD_MATCH, 0300743288131, 1 Er Tablets 1x100 Mfg. Abbott Laboratories 240 mg,1 count, Abbott Laboratories, JLI2V7, Healthcare, null, null, null]",
                Arrays.toString(result));
    }

    @Test
    public void testNormalizeGtinCode() throws Exception {
        assertEquals("0000000000002", ProductMatchTransformer.normalizeGtinCode("2"));
        assertEquals("0300743288131", ProductMatchTransformer.normalizeGtinCode("0300743288131"));
        assertEquals("0300743288131", ProductMatchTransformer.normalizeGtinCode("300743288131"));
        assertEquals("0000123423499", ProductMatchTransformer.normalizeGtinCode("12-34_234 9_9 "));
        assertEquals("0000123423499", ProductMatchTransformer.normalizeGtinCode("123423499"));
        assertEquals(null, ProductMatchTransformer.normalizeGtinCode(""));
        assertEquals(null, ProductMatchTransformer.normalizeGtinCode(null));
    }
}
