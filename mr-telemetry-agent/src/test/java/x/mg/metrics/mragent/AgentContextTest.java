package x.mg.metrics.mragent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import x.mg.metrics.mragent.counter.TaskIdentity;

import static org.junit.jupiter.api.Assertions.*;

class AgentContextTest {

    @AfterEach
    void reset() {
        AgentContext.reset();
    }

    @Test
    void testSingletonInitialization() {
        AgentContext ctx1 = AgentContext.getOrInit();
        AgentContext ctx2 = AgentContext.getOrInit();
        assertSame(ctx1, ctx2);
    }

    @Test
    void testReset() {
        AgentContext.getOrInit();
        AgentContext.reset();
        // Should be able to init again after reset
        AgentContext ctx = AgentContext.getOrInit();
        assertNotNull(ctx);
    }

    @Test
    void testDetectTaskType() {
        AgentContext ctx = AgentContext.getOrInit();

        assertEquals("map", ctx.detectTaskType("x.mg.MyMapper.run(org.apache.hadoop.mapreduce.Mapper$Context)"));
        assertEquals("reduce", ctx.detectTaskType("x.mg.MyReducer.run(org.apache.hadoop.mapreduce.Reducer$Context)"));
        assertEquals("map", ctx.detectTaskType("Mapper.run"));
        assertEquals("reduce", ctx.detectTaskType("Reducer.run"));
        assertEquals("unknown", ctx.detectTaskType("SomethingElse.run"));
        assertEquals("unknown", ctx.detectTaskType(null));
    }

    @Test
    void testOnRunEnterExitDoesNotThrowWithNullContext() {
        AgentContext ctx = AgentContext.getOrInit();
        // Should not throw even with null context (graceful handling)
        assertDoesNotThrow(() -> ctx.onRunEnter(null, "Mapper.run(Context)"));
        assertDoesNotThrow(() -> ctx.onRunExit(null, "Mapper.run(Context)"));
    }
}
