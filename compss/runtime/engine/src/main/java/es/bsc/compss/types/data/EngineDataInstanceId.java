package es.bsc.compss.types.data;

import es.bsc.compss.types.data.info.DataVersion;


public class EngineDataInstanceId extends DataInstanceId {

    private final DataVersion version;


    public EngineDataInstanceId(int dataId, int versionId, DataVersion version) {
        super(dataId, versionId);
        this.version = version;
    }

    public DataVersion getVersion() {
        return version;
    }
}
