package x.mg.metrics.flink.filter;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

public class MetricNameFilter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Pattern> includes;
    private final List<Pattern> excludes;

    public MetricNameFilter(List<String> includePatterns, List<String> excludePatterns) {
        this.includes = compilePatterns(includePatterns);
        this.excludes = compilePatterns(excludePatterns);
    }

    public boolean test(String metricName) {
        if (metricName == null) return false;

        boolean included = includes.isEmpty() || includes.stream().anyMatch(p -> p.matcher(metricName).find());
        boolean excluded = excludes.stream().anyMatch(p -> p.matcher(metricName).find());
        return included && !excluded;
    }

    private static List<Pattern> compilePatterns(List<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        java.util.List<Pattern> compiled = new java.util.ArrayList<>();
        for (String p : patterns) {
            if (p != null && !p.isEmpty()) {
                compiled.add(Pattern.compile(p));
            }
        }
        return compiled;
    }
}
