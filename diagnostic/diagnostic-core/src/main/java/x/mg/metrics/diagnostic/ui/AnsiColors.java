package x.mg.metrics.diagnostic.ui;

/**
 * ANSI 颜色定义
 */
public final class AnsiColors {
    public static final String RESET = "\u001B[0m";
    public static final String BLACK = "\u001B[30m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    public static final String MAGENTA = "\u001B[35m";
    public static final String CYAN = "\u001B[36m";
    public static final String WHITE = "\u001B[37m";

    public static final String BOLD = "\u001B[1m";
    public static final String DIM = "\u001B[2m";
    public static final String ITALIC = "\u001B[3m";
    public static final String UNDERLINE = "\u001B[4m";

    // 状态图标
    public static final String CHECK = "✓";
    public static final String CROSS = "✗";
    public static final String WARNING = "⚠";
    public static final String INFO = "ℹ";
    public static final String ARROW = "▶";
    public static final String SKIP = "⊘";

    private AnsiColors() {
        // 工具类，禁止实例化
    }
}
