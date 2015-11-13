package org.datacleaner.extension.productmatch.ui;

import javax.swing.JComponent;

import org.datacleaner.api.Renderer;
import org.datacleaner.api.RendererBean;
import org.datacleaner.api.RendererPrecedence;
import org.datacleaner.extension.productmatch.ProductMatchResult;
import org.datacleaner.result.renderer.SwingRenderingFormat;

@RendererBean(SwingRenderingFormat.class)
public class ProductMatchResultSwingRenderer implements Renderer<ProductMatchResult, JComponent> {

    @Override
    public RendererPrecedence getPrecedence(ProductMatchResult result) {
        return RendererPrecedence.HIGH;
    }

    @Override
    public JComponent render(ProductMatchResult result) {
        return new ProductMatchResultPanel(result);
    }

}
