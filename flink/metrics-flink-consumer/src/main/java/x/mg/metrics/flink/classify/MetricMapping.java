package x.mg.metrics.flink.classify;

public class MetricMapping {
    private final MetricCategory category;
    private final String columnName;
    private final boolean histogram;

    public MetricMapping(MetricCategory category, String columnName, boolean histogram) {
        this.category = category;
        this.columnName = columnName;
        this.histogram = histogram;
    }

    public MetricCategory getCategory() { return category; }
    public String getColumnName() { return columnName; }
    public boolean isHistogram() { return histogram; }
}
