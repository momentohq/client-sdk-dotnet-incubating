using System;
namespace Momento.Sdk.Incubating.Requests
{
    public record struct CollectionUpdateTtl(TimeSpan? ttl = null, bool refreshTtl = true)
    {
        public static CollectionUpdateTtl noRefreshOnUpdates()
        {
            return new CollectionUpdateTtl(ttl: null, refreshTtl: false);
        }
        public static CollectionUpdateTtl fromTimespan(TimeSpan ttl)
        {
            return new CollectionUpdateTtl(ttl: ttl);
        }

        public CollectionUpdateTtl refreshTtlOnUpdates(bool refreshTtl)
        {
            return new CollectionUpdateTtl(this.ttl, refreshTtl);
        }
    }
}

