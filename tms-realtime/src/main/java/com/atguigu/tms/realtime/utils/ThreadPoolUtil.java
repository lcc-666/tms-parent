package com.atguigu.tms.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static volatile ThreadPoolExecutor poolExecutor;

    public static synchronized ThreadPoolExecutor getInstance() {
        if (poolExecutor == null) {
            synchronized (ThreadPoolUtil.class){
                if (poolExecutor == null) {
                    poolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }

        return poolExecutor;
    }
}
