package org.elasticsearch.action.search;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Settings for search
 */
public class SearchSettings { // NOTE:htt, search 查询场景下配置

    public static final Setting<TimeValue> SEARCH_RPCT_TIMEOUT =
            Setting.positiveTimeSetting("search.rpc.timeout", TimeValue.timeValueSeconds(10),
                    Property.Dynamic, Property.NodeScope); // NOTE:htt, 默认search rpc超时时间为10s

    private final ClusterService clusterService;

    private TimeValue searchRpcTimeout; // NOTE:htt, search rpc调用的超时时间, 0代表一直等待

    public SearchSettings(ClusterService clusterService) {
        this.clusterService = clusterService;
        Settings settings = clusterService.getSettings();
        this.searchRpcTimeout = SEARCH_RPCT_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SEARCH_RPCT_TIMEOUT, value -> {
            searchRpcTimeout = value;
        });
    }

    public void setSearchRpcTimeout(TimeValue searchRpcTimeout) {
        this.searchRpcTimeout = searchRpcTimeout;
    }

    public TimeValue getSearchRpcTimeout() {
        return searchRpcTimeout;
    }

    public final ClusterService getClusterService() {
        return clusterService;
    }
}
