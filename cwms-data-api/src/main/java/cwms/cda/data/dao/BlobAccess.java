package cwms.cda.data.dao;

import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

public interface BlobAccess {
    @NotNull Blobs getBlobs(@Nullable String cursor, int pageSize, @Nullable String officeId, @Nullable String like);

    Optional<Blob> getByUniqueName(String id, String office);

    void getBlob(String id, String office, StreamConsumer consumer, @Nullable Long offset, @Nullable Long end);

    void create(Blob blob, boolean failIfExists, boolean ignoreNulls);

    void update(Blob blob, boolean ignoreNulls);

    void delete(String office, String id);
}
