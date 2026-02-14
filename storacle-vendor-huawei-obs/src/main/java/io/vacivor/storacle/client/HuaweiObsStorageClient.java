package io.vacivor.storacle.client;

import java.util.Objects;

import com.obs.services.ObsClient;
import com.obs.services.model.CopyObjectRequest;
import com.obs.services.model.CopyObjectResult;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.ObjectSummary;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.StorageObject;

import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class HuaweiObsStorageClient implements ObjectStorageClient {

  private final ObsClient client;

  public HuaweiObsStorageClient(ObsClient client) {
    this.client = Objects.requireNonNull(client, "client must not be null");
  }

  @Override
  public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
    Objects.requireNonNull(path, "path must not be null");
    Objects.requireNonNull(content, "content must not be null");
    ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;

    try {
      com.obs.services.model.ObjectMetadata obsMetadata = new com.obs.services.model.ObjectMetadata();
      if (safeMetadata.contentType() != null) {
        obsMetadata.setContentType(safeMetadata.contentType());
      }
      if (safeMetadata.contentLength() != null && safeMetadata.contentLength() >= 0) {
        obsMetadata.setContentLength(safeMetadata.contentLength());
      }
      if (safeMetadata.cacheControl() != null) {
        obsMetadata.setCacheControl(safeMetadata.cacheControl());
      }
      if (safeMetadata.contentDisposition() != null) {
        obsMetadata.setContentDisposition(safeMetadata.contentDisposition());
      }
      if (!safeMetadata.userMetadata().isEmpty()) {
        Map<String, String> userMetadata = safeMetadata.userMetadata();
        HashMap<String, Object> map = new HashMap<>(userMetadata);
        obsMetadata.setUserMetadata(map);
      }

      client.putObject(path.bucket(), path.key(), content, obsMetadata);
      return new ObjectWriteResult(path, null, null);
    } catch (Exception e) {
      throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
    }
  }

  @Override
  public StorageObject get(ObjectPath path) {
    Objects.requireNonNull(path, "path must not be null");
    try {
      ObsObject object = client.getObject(path.bucket(), path.key());
      com.obs.services.model.ObjectMetadata meta = object.getMetadata();
      ObjectMetadata metadata = ObjectMetadata.builder()
          .contentType(meta.getContentType())
          .contentLength(meta.getContentLength())
          .cacheControl(meta.getCacheControl())
          .contentDisposition(meta.getContentDisposition())
          .build();
      return new StorageObject(path, metadata, object.getObjectContent());
    } catch (Exception e) {
      throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
    }
  }

  @Override
  public boolean delete(ObjectPath path) {
    Objects.requireNonNull(path, "path must not be null");
    try {
      client.deleteObject(path.bucket(), path.key());
      return true;
    } catch (Exception e) {
      throw new StorageException("Failed to delete object: " + path.bucket() + "/" + path.key(), e);
    }
  }

  @Override
  public boolean exists(ObjectPath path) {
    Objects.requireNonNull(path, "path must not be null");
    try {
      return client.doesObjectExist(path.bucket(), path.key());
    } catch (Exception e) {
      throw new StorageException(
          "Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
    }
  }

  @Override
  public ListObjectsPage list(ListObjectsRequest request) {
    Objects.requireNonNull(request, "request must not be null");
    try {
      com.obs.services.model.ListObjectsRequest obsRequest = new com.obs.services.model.ListObjectsRequest(
          request.bucket());
      obsRequest.setPrefix(request.prefix());
      obsRequest.setMaxKeys(request.maxKeys());
      obsRequest.setMarker(request.continuationToken());

      ObjectListing listing = client.listObjects(obsRequest);
      List<ObjectSummary> summaries = listing.getObjects().stream()
          .map(summary -> new ObjectSummary(
              ObjectPath.of(request.bucket(), summary.getObjectKey()),
              summary.getMetadata() == null ? 0 : summary.getMetadata().getContentLength(),
              null,
              null))
          .toList();

      return new ListObjectsPage(summaries, listing.getNextMarker(), listing.isTruncated());
    } catch (Exception e) {
      throw new StorageException("Failed to list objects for bucket: " + request.bucket(), e);
    }
  }

  @Override
  public ObjectWriteResult copy(ObjectPath source, ObjectPath target,
      ObjectMetadata metadataOverride) {
    Objects.requireNonNull(source, "source must not be null");
    Objects.requireNonNull(target, "target must not be null");
    try {
      CopyObjectRequest copyRequest = new CopyObjectRequest(source.bucket(), source.key(),
          target.bucket(), target.key());
      if (metadataOverride != null) {
        com.obs.services.model.ObjectMetadata meta = new com.obs.services.model.ObjectMetadata();
        if (metadataOverride.contentType() != null) {
          meta.setContentType(metadataOverride.contentType());
        }
        if (metadataOverride.cacheControl() != null) {
          meta.setCacheControl(metadataOverride.cacheControl());
        }
        if (metadataOverride.contentDisposition() != null) {
          meta.setContentDisposition(metadataOverride.contentDisposition());
        }
        if (!metadataOverride.userMetadata().isEmpty()) {
          meta.setUserMetadata(new HashMap<>(metadataOverride.userMetadata()));
        }
        copyRequest.setNewObjectMetadata(meta);
      }
      CopyObjectResult result = client.copyObject(copyRequest);
      return new ObjectWriteResult(target, result.getEtag(), null);
    } catch (Exception e) {
      throw new StorageException("Failed to copy object: " + source.bucket() + "/" + source.key(),
          e);
    }
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      throw new StorageException("Failed to close Huawei OBS client", e);
    }
  }

  @Override
  public <T> T unwrap(Class<T> type) {
    if (type.isInstance(client)) {
      return type.cast(client);
    }
    return ObjectStorageClient.super.unwrap(type);
  }
}
