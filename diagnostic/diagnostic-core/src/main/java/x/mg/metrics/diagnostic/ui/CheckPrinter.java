package x.mg.metrics.diagnostic.ui;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.checks.CheckItem;

import java.io.PrintWriter;
import java.util.List;

/**
 * 检查结果打印工具
 */
public final class CheckPrinter {

    private CheckPrinter() {}

    public static void print(Terminal terminal, List<CheckItem> items) {
        PrintWriter pw = terminal.writer();
        for (CheckItem item : items) {
            switch (item.getStatus()) {
                case OK:
                    pw.println(AnsiColors.GREEN + "  ✓ " + AnsiColors.RESET + item.getMessage());
                    break;
                case WARNING:
                    pw.println(AnsiColors.YELLOW + "  ⚠ " + AnsiColors.RESET + item.getMessage());
                    if (item.getRemediation() != null) {
                        for (String line : item.getRemediation().split("\n")) {
                            pw.println(AnsiColors.DIM + "    " + line + AnsiColors.RESET);
                        }
                    }
                    break;
                case FAILED:
                    pw.println(AnsiColors.RED + "  ✗ " + AnsiColors.RESET + item.getMessage());
                    if (item.getRemediation() != null) {
                        for (String line : item.getRemediation().split("\n")) {
                            pw.println(AnsiColors.DIM + "    " + line + AnsiColors.RESET);
                        }
                    }
                    break;
                case SKIPPED:
                    pw.println(AnsiColors.DIM + "  ⊘ " + item.getMessage() + AnsiColors.RESET);
                    break;
            }
        }
        pw.println();
        pw.flush();
    }
}
