package x.mg.metrics.diagnostic.checks;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 单项检查结果
 */
public class CheckItem {
    public enum Status { OK, WARNING, FAILED, SKIPPED }

    private final Status status;
    private final String message;
    private final String remediation;

    public static CheckItem ok(String message) {
        return new CheckItem(Status.OK, message, null);
    }

    public static CheckItem warn(String message, String remediation) {
        return new CheckItem(Status.WARNING, message, remediation);
    }

    public static CheckItem fail(String message, String remediation) {
        return new CheckItem(Status.FAILED, message, remediation);
    }

    public static CheckItem skip(String message) {
        return new CheckItem(Status.SKIPPED, message, null);
    }

    private CheckItem(Status status, String message, String remediation) {
        this.status = status;
        this.message = message;
        this.remediation = remediation;
    }

    public Status getStatus() { return status; }
    public String getMessage() { return message; }
    public String getRemediation() { return remediation; }
    public boolean isOk() { return status == Status.OK; }
    public boolean isFailed() { return status == Status.FAILED; }
}
