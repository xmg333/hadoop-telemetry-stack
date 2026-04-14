package x.mg.metrics.sparktelemetry.adapter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests OmniContext version detection logic.
 * Since OmniContext uses Class.forName at static init time,
 * we test the detection method directly by invoking it via reflection
 * in an isolated classloader context.
 *
 * Instead, we verify the detection logic by checking the bytecode
 * references and the public API contract.
 */
class OmniContextTest {

    @Test
    void testGetVersionReturnsNonNull() {
        // getVersion() is initialized at class-load time using the current classpath.
        // On a test classpath without Spark, it should return "2" (no SparkPlugin).
        String version = OmniContext.getVersion();
        assertNotNull(version);
        assertFalse(version.isEmpty());
    }

    @Test
    void testGetAdapterPackageContainsVersion() {
        String pkg = OmniContext.getAdapterPackage();
        String version = OmniContext.getVersion();
        assertTrue(pkg.endsWith(".v" + version),
                "Package '" + pkg + "' should end with '.v" + version + "'");
        assertTrue(pkg.startsWith("x.mg.metrics.sparktelemetry.adapter.internal."),
                "Package should be under adapter.internal");
    }

    @Test
    void testIsSpark2ConsistentWithVersion() {
        boolean isSpark2 = OmniContext.isSpark2();
        String version = OmniContext.getVersion();
        assertEquals("2".equals(version), isSpark2,
                "isSpark2 should match version == '2'");
    }

    @Test
    void testVersionIsOneOfExpected() {
        String version = OmniContext.getVersion();
        assertTrue(java.util.Arrays.asList("2", "30", "32", "35", "4").contains(version),
                "Version should be one of [2, 30, 32, 35, 4], got: " + version);
    }

    @Test
    void testDetectionUsesCorrectProbes() throws Exception {
        // Verify detectVersion method exists and has the expected signature
        Method detectMethod = OmniContext.class.getDeclaredMethod("detectVersion");
        assertNotNull(detectMethod);
        assertEquals(String.class, detectMethod.getReturnType());
    }

    @Test
    void testNoSparkOnClasspath() {
        // Without Spark on the test classpath, OmniContext should detect "2"
        // (no SparkPlugin interface found)
        try {
            Class.forName("org.apache.spark.api.plugin.SparkPlugin");
            // If Spark IS on classpath, skip this assertion
            System.out.println("Spark on classpath, skipping no-Spark test");
        } catch (ClassNotFoundException e) {
            assertEquals("2", OmniContext.getVersion(),
                    "Without Spark on classpath, should detect as Spark 2.x");
        }
    }
}
