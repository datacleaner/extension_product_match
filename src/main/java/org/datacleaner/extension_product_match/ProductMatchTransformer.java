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

package org.datacleaner.extension_product_match;

import org.datacleaner.api.Categorized;
import org.datacleaner.api.Configured;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.OutputColumns;
import org.datacleaner.api.Transformer;
import org.datacleaner.components.categories.ImproveSuperCategory;
import org.datacleaner.components.categories.ReferenceDataCategory;


@javax.inject.Named(value="Product Matching")
@org.datacleaner.api.Description(value="Identify the various commerial products by their name or by GTIN-13 code")
@Categorized(superCategory = ImproveSuperCategory.class, value = ReferenceDataCategory.class)
public class ProductMatchTransformer  implements Transformer {
    
    public static enum ProductEnum {
        PRODUCT_NAME,
        BRAND_NAME,
        GTIN_CODE
        
    }
    @Configured
    InputColumn<String> searchInput;
    
    
    @Override
    public OutputColumns getOutputColumns() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] transform(InputRow arg0) {
        // TODO Auto-generated method stub
        return null;
    }

}
