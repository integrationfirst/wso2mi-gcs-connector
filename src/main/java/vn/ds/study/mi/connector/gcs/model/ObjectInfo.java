package vn.ds.study.mi.connector.gcs.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class ObjectInfo {
    private String key;
    private String md5;
    private long sizeInBytes;
}
