package x.mg.metrics.diagnostic.state;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.config.DiagnosticConfig;
import x.mg.metrics.diagnostic.report.DiagnosticReport;

import java.util.ArrayList;
import java.util.List;

/**
 * 诊断上下文
 */
public class DiagnosticContext {

    private final DiagnosticConfig config;
    private final Terminal terminal;
    private final List<String> issues = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final List<String> info = new ArrayList<>();
    private DiagnosticReport report;

    public DiagnosticContext(DiagnosticConfig config, Terminal terminal) {
        this.config = config;
        this.terminal = terminal;
    }

    public DiagnosticConfig getConfig() {
        return config;
    }

    public Terminal getTerminal() {
        return terminal;
    }

    public void addIssue(String issue) {
        issues.add(issue);
    }

    public void addWarning(String warning) {
        warnings.add(warning);
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

    public void setReport(DiagnosticReport report) {
        this.report = report;
    }

    public DiagnosticReport getReport() {
        return report;
    }

    public boolean hasIssues() {
        return !issues.isEmpty();
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }
}
