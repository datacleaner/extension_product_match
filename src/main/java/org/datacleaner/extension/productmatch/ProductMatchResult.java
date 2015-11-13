package org.datacleaner.extension.productmatch;

import java.util.Map;

import org.datacleaner.api.AnalyzerResult;

public class ProductMatchResult implements AnalyzerResult {

    private static final long serialVersionUID = 1L;

    private final Map<String, ? extends Number> _matchStatuses;
    private final Map<String, ? extends Number> _segments;

    public ProductMatchResult(Map<String, ? extends Number> matchStatuses, Map<String, ? extends Number> segments) {
        _matchStatuses = matchStatuses;
        _segments = segments;
    }

    public Map<String, ? extends Number> getMatchStatuses() {
        return _matchStatuses;
    }

    public Map<String, ? extends Number> getSegments() {
        return _segments;
    }
}
