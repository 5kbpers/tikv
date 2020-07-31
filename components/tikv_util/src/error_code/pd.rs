define_error_codes!(
    "KV:PD:",

    IO => ("IO", "", ""),
    CLUSTER_BOOTSTRAPPED => ("ClusterBootstraped", "", ""),
    CLUSTER_NOT_BOOTSTRAPPED => ("ClusterNotBootstraped", "", ""),
    INCOMPATIBLE => ("Imcompatible", "", ""),
    GRPC => ("gRPC", "", ""),
    REGION_NOT_FOUND => ("RegionNotFound", "", ""),
    STORE_TOMBSTONE => ("StoreTombstone", "", ""),
    UNDETERMINED => ("Undetermined", "", "")
);
