package org.sunrider.inboxhousekeeping.aop.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component
@RequiredArgsConstructor
public class JobMetricsAspect {

    private final MeterRegistry meterRegistry;

    @Around("execution(* org.sunrider.inboxhousekeeping..*Job.run(..))")
    public Object measureJob(ProceedingJoinPoint pjp) throws Throwable {
        String status = "error";

        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            Object result = pjp.proceed();
            status = "success";
            return result;
        } finally {
            sample.stop(Timer
                .builder("job.execution")
                .tag("job", pjp.getSignature().getDeclaringType().getSimpleName())
                .tag("status", status)
                .register(meterRegistry));
        }
    }
}
