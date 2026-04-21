package x.mg.metrics.diagnostic.report;

import java.util.ArrayList;
import java.util.List;

/**
 * 诊断报告
 */
public class DiagnosticReport {

    private final List<String> issues = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final List<String> info = new ArrayList<>();
    private OverallStatus overallStatus = OverallStatus.UNKNOWN;

    public enum OverallStatus {
        HEALTHY("健康"),
        WARNING("警告"),
        ERROR("错误"),
        UNKNOWN("未知");

        private final String displayName;

        OverallStatus(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    public void addIssue(String issue) {
        issues.add(issue);
        if (overallStatus == OverallStatus.HEALTHY || overallStatus == OverallStatus.UNKNOWN) {
            overallStatus = OverallStatus.ERROR;
        }
    }

    public void addWarning(String warning) {
        warnings.add(warning);
        if (overallStatus == OverallStatus.HEALTHY || overallStatus == OverallStatus.UNKNOWN) {
            overallStatus = OverallStatus.WARNING;
        }
    }

    public void addInfo(String info) {
        this.info.add(info);
    }

    public List<String> getIssues() {
        return issues;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public List<String> getInfo() {
        return info;
    }

    public OverallStatus getOverallStatus() {
        return overallStatus;
    }

    public boolean hasIssues() {
        return !issues.isEmpty();
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }
}
