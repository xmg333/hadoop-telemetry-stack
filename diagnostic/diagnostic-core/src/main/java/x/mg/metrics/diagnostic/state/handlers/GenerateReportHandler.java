package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.report.DiagnosticReport;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.state.StateHandler;
import x.mg.metrics.diagnostic.ui.AnsiColors;

/**
 * 生成报告状态处理器
 */
public class GenerateReportHandler implements StateHandler {

    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        DiagnosticReport report = new DiagnosticReport();

        // 收集所有问题
        for (String issue : context.getIssues()) {
            report.addIssue(issue);
        }
        for (String warning : context.getWarnings()) {
            report.addWarning(warning);
        }
        for (String info : context.getInfo()) {
            report.addInfo(info);
        }

        context.setReport(report);

        // 打印报告摘要
        printReportSummary(context, report);

        // 状态转换
        return DiagnosticState.EXIT_SUCCESS;
    }

    private void printReportSummary(DiagnosticContext context, DiagnosticReport report) {
        context.getTerminal().writer().println();
        context.getTerminal().writer().println(AnsiColors.BOLD + "════════════════════════════════════════════════════════" + AnsiColors.RESET);
        context.getTerminal().writer().println(AnsiColors.BOLD + "诊断结果" + AnsiColors.RESET);
        context.getTerminal().writer().println(AnsiColors.BOLD + "════════════════════════════════════════════════════════" + AnsiColors.RESET);
        context.getTerminal().writer().println();

        // 整体状态
        String statusText = report.getOverallStatus().getDisplayName();
        String statusColor = getStatusColor(report.getOverallStatus());
        context.getTerminal().writer().println(
            "整体状态：" + statusColor + statusText + AnsiColors.RESET
        );
        context.getTerminal().writer().println();

        // 问题列表
        if (!report.getIssues().isEmpty()) {
            context.getTerminal().writer().println(AnsiColors.RED + "发现问题:" + AnsiColors.RESET);
            for (String issue : report.getIssues()) {
                context.getTerminal().writer().println(AnsiColors.RED + "  ✗ " + AnsiColors.RESET + issue);
            }
            context.getTerminal().writer().println();
        }

        // 警告列表
        if (!report.getWarnings().isEmpty()) {
            context.getTerminal().writer().println(AnsiColors.YELLOW + "警告:" + AnsiColors.RESET);
            for (String warning : report.getWarnings()) {
                context.getTerminal().writer().println(AnsiColors.YELLOW + "  ⚠ " + AnsiColors.RESET + warning);
            }
            context.getTerminal().writer().println();
        }

        context.getTerminal().writer().println(AnsiColors.BOLD + "════════════════════════════════════════════════════════" + AnsiColors.RESET);
        context.getTerminal().writer().flush();
    }

    private String getStatusColor(DiagnosticReport.OverallStatus status) {
        switch (status) {
            case HEALTHY: return AnsiColors.GREEN;
            case WARNING: return AnsiColors.YELLOW;
            case ERROR: return AnsiColors.RED;
            default: return AnsiColors.WHITE;
        }
    }

    @Override
    public String getName() {
        return "生成报告";
    }
}
