package org.datacleaner.extension.productmatch;

import static org.junit.Assert.assertEquals;

import org.datacleaner.api.InputColumn;
import org.datacleaner.data.MockInputColumn;
import org.datacleaner.data.MockInputRow;
import org.junit.Test;

import cern.colt.Arrays;

public class ProductMatchTransformerTest {

    private final MockInputColumn<String> product = new MockInputColumn<>("product");
    private final MockInputColumn<String> brand = new MockInputColumn<>("brand");
    private final MockInputColumn<String> description = new MockInputColumn<>("product");

    @Test
    public void testPlainTextMatchCocaCola() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        transformer.inputColumns = new InputColumn[] { description };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_DESCRIPTION_TEXT };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(description, "Coca-cola"));
        assertEquals(
                "[GOOD_MATCH, 7894900011517, Coca Cola 2 litros||Refrigerantes | COCA COLA 2 LTRS, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]",
                Arrays.toString(result));

        result = transformer.transform(new MockInputRow().put(description, "Coca cola zero 1 liter"));
        assertEquals(
                "[GOOD_MATCH, 7894900701753, COCA COLA ZERO 1,, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]",
                Arrays.toString(result));
    }

    @Test
    public void testMatchOnProductAndBrandCocaCola() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        transformer.inputColumns = new InputColumn[] { product, brand };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_NAME,
                ProductMatchTransformer.ProductFieldType.BRAND_NAME };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(brand, "Coca-cola").put(product, "Free"));
        assertEquals(
                "[GOOD_MATCH, 0049000006131, Caffeine Free, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]",
                Arrays.toString(result));

        result = transformer.transform(new MockInputRow().put(brand, "Coca-cola").put(product, "Life"));
        // TODO: Should probably not be a GOOD match. Maybe a POTENTIAL match?
        assertEquals(
                "[GOOD_MATCH, 0049000000061, Cola With Cherry Flavor, Coca-Cola, 5MRM4M, Food/Beverage/Tobacco, null, null, null]",
                Arrays.toString(result));
    }

    @Test
    public void testMatchOnProductAndBrandLego() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        transformer.inputColumns = new InputColumn[] { product, brand, description };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_NAME,
                ProductMatchTransformer.ProductFieldType.BRAND_NAME,
                ProductMatchTransformer.ProductFieldType.PRODUCT_DESCRIPTION_TEXT };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(brand, "Lego").put(product, "Star wars destroyer")
                .put(description, "The elefant-like thing from the Star Wars movies"));
        assertEquals(
                "[GOOD_MATCH, 0082493500007, Star Wars Imperial Star Destroyer, Lego, GSD9GK, Toys/Games, null, null, null]",
                Arrays.toString(result));
    }

    @Test
    public void testNoMatchUnknownTerms() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        transformer.inputColumns = new InputColumn[] { description };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_DESCRIPTION_TEXT };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(description, "helloworldabracadabra"));
        assertEquals("[NO_MATCH, null, null, null, null, null, null, null, null]", Arrays.toString(result));
    }

    @Test
    public void testNoMatchNoQuery() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        transformer.inputColumns = new InputColumn[] { description };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.PRODUCT_DESCRIPTION_TEXT };

        transformer.init();

        Object[] result = transformer.transform(new MockInputRow().put(description, ""));
        assertEquals("[SKIPPED, null, null, null, null, null, null, null, null]", Arrays.toString(result));

        result = transformer.transform(new MockInputRow().put(description, null));
        assertEquals("[SKIPPED, null, null, null, null, null, null, null, null]", Arrays.toString(result));
    }

    @Test
    public void testGtinLookup() throws Exception {
        final ProductMatchTransformer transformer = new ProductMatchTransformer();

        transformer.inputColumns = new InputColumn[] { description };
        transformer.inputMapping = new ProductMatchTransformer.ProductFieldType[] {
                ProductMatchTransformer.ProductFieldType.GTIN_CODE };

        transformer.init();
        Object[] result = transformer.transform(new MockInputRow().put(description, "0300743288131"));
        assertEquals(
                "[GOOD_MATCH, 0300743288131, 1 Er Tablets 1x100 Mfg. Abbott Laboratories 240 mg,1 count, Abbott Laboratories, JLI2V7, Healthcare, null, null, null]",
                Arrays.toString(result));

        result = transformer.transform(new MockInputRow().put(description, "300743288131"));
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
