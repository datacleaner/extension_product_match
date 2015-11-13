package org.datacleaner.extension.productmatch.ui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Image;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.swing.JFrame;
import javax.swing.JSplitPane;
import javax.swing.border.EmptyBorder;

import org.datacleaner.extension.productmatch.ProductMatchResult;
import org.datacleaner.extension.productmatch.ProductMatchTransformer;
import org.datacleaner.panels.DCPanel;
import org.datacleaner.util.ChartUtils;
import org.datacleaner.util.ImageManager;
import org.datacleaner.util.LookAndFeelManager;
import org.datacleaner.util.WidgetUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;

public class ProductMatchResultPanel extends DCPanel {

    private static final long serialVersionUID = 1L;

    private static final Image WATERMARK_IMAGE = ImageManager.get().getImage("images/pod_watermark.png",
            ProductMatchResultPanel.class.getClassLoader());

    public ProductMatchResultPanel(ProductMatchResult result) {
        super(WATERMARK_IMAGE, 100, 100);

        final JFreeChart matchStatusChart = createPieChart("Match status", result.getMatchStatuses());

        final JFreeChart segmentChart = createBarChart("Product segment", result.getSegments());

        Component left = WidgetUtils.decorateWithShadow(new ChartPanel(matchStatusChart));
        Component right = WidgetUtils.decorateWithShadow(new ChartPanel(segmentChart));

        final JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, right);
        split.setDividerLocation(450);

        setBorder(new EmptyBorder(0, 0, 266, 0));
        setLayout(new BorderLayout());
        add(split, BorderLayout.CENTER);
    }

    private JFreeChart createBarChart(String name, Map<String, ? extends Number> map) {
        final String categoryAxisLabel = null;
        final String valueAxisLabel = null;
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        final Set<String> keys = map.keySet();
        for (String key : keys) {
            final Number value = map.get(key);
            dataset.addValue(value, key, name);
        }

        final PlotOrientation orientation = PlotOrientation.VERTICAL;
        final boolean legend = true;
        final boolean tooltips = true;
        final boolean urls = false;
        final JFreeChart chart = ChartFactory.createBarChart(name, categoryAxisLabel, valueAxisLabel, dataset,
                orientation, legend, tooltips, urls);
        ChartUtils.applyStyles(chart);
        return chart;
    }

    private JFreeChart createPieChart(String name, Map<String, ? extends Number> map) {
        final DefaultPieDataset dataset = new DefaultPieDataset();
        final Set<String> keys = map.keySet();
        for (String key : keys) {
            final Number value = map.get(key);
            addValue(dataset, key, value);
        }

        final JFreeChart chart = ChartFactory.createPieChart(name, dataset, false, true, false);
        ChartUtils.applyStyles(chart);
        return chart;
    }

    private void addValue(DefaultPieDataset dataset, String label, Number value) {
        if (value == null) {
            return;
        }
        dataset.setValue(label, value);
    }

    public static void main(String[] args) {
        LookAndFeelManager.get().init();

        final Map<String, Number> matchStatuses = new HashMap<>();
        matchStatuses.put(ProductMatchTransformer.MATCH_STATUS_GOOD, 20);
        matchStatuses.put(ProductMatchTransformer.MATCH_STATUS_POTENTIAL, 11);
        matchStatuses.put(ProductMatchTransformer.MATCH_STATUS_SKIPPED, 5);

        final Map<String, Number> segments = new HashMap<>();
        segments.put("Food", 23);
        segments.put("Cars", 5);

        final ProductMatchResult result = new ProductMatchResult(matchStatuses, segments);

        final ProductMatchResultPanel panel = new ProductMatchResultPanel(result);
        JFrame frame = new JFrame("test frame");
        frame.getContentPane().add(panel);
        frame.pack();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        frame.setVisible(true);
    }
}
