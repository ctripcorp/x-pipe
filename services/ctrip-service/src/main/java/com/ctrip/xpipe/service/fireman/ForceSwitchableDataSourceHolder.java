package com.ctrip.xpipe.service.fireman;

import com.ctrip.platform.dal.dao.datasource.ForceSwitchableDataSource;

import java.util.concurrent.atomic.AtomicReference;

public class ForceSwitchableDataSourceHolder {

    private static ForceSwitchableDataSourceHolder ourInstance = new ForceSwitchableDataSourceHolder();

    private AtomicReference<ForceSwitchableDataSource> m_dataSource = new AtomicReference<>();

    public static ForceSwitchableDataSourceHolder getInstance() {
        return ourInstance;
    }

    private ForceSwitchableDataSourceHolder() {
    }

    public ForceSwitchableDataSource getDataSource() {
        return m_dataSource.get();
    }

    public void setDataSource(ForceSwitchableDataSource dataSource) {
        m_dataSource.set(dataSource);
    }
}
