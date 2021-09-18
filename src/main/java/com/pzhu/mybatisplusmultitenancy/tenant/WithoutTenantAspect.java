package com.pzhu.mybatisplusmultitenancy.tenant;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;


@Slf4j
@Aspect
@AllArgsConstructor
public class WithoutTenantAspect {

    @Around("@annotation(com.pzhu.mybatisplusmultitenancy.annotation.WithoutTenant)")
    public Object doWithoutTenant(ProceedingJoinPoint proceedingJoinPoint) {
        final String tenant = TenantContext.getCurrentTenant();
        try {
            TenantContext.clear();
            Object object =  proceedingJoinPoint.proceed();
            return object;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            TenantContext.setCurrentTenant(tenant);
        }
        return null;
    }
}
