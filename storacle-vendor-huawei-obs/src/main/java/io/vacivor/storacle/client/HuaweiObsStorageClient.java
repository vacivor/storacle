package io.vacivor.storacle.client;

import com.obs.services.ObsClient;
import com.obs.services.model.CopyObjectRequest;
import com.obs.services.model.CopyObjectResult;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import io.vacivor.storacle.AbstractObjectStorageClient;
import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectMetadataMapper;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageValueFactory;
import io.vacivor.storacle.ObjectSummary;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.StorageObject;

import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class HuaweiObsStorageClient extends AbstractObjectStorageClient {

  private final ObsClient client;

  public HuaweiObsStorageClient(ObsClient client) {
    this.client = java.util.Objects.requireNonNull(client, "client must not be null");
  }

  @Override
  public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
    path = requirePath(path);
    content = requireContent(content);
    ObjectMetadata safeMetadata = safeMetadata(metadata);

    try {
      com.obs.services.model.ObjectMetadata obsMetadata = new com.obs.services.model.ObjectMetadata();
      ObjectMetadataMapper.apply(
          safeMetadata,
          obsMetadata::setContentType,
          obsMetadata::setContentLength,
          obsMetadata::setCacheControl,
          obsMetadata::setContentDisposition,
          userMetadata -> obsMetadata.setUserMetadata(new HashMap<>(userMetadata)));

      client.putObject(path.bucket(), path.key(), content, obsMetadata);
      return new ObjectWriteResult(path, null, null);
    } catch (Exception e) {
      throw objectFailure("put", path, e);
    }
  }

  @Override
  public StorageObject get(ObjectPath path) {
    path = requirePath(path);
    try {
      ObsObject object = client.getObject(path.bucket(), path.key());
      com.obs.services.model.ObjectMetadata meta = object.getMetadata();
      ObjectMetadata metadata = ObjectMetadataMapper.from(
          meta::getContentType,
          meta::getContentLength,
          meta::getCacheControl,
          meta::getContentDisposition,
          Map::of);
      return ObjectStorageValueFactory.storageObject(path, metadata, object.getObjectContent());
    } catch (Exception e) {
      throw objectFailure("get", path, e);
    }
  }

  @Override
  public boolean delete(ObjectPath path) {
    path = requirePath(path);
    try {
      client.deleteObject(path.bucket(), path.key());
      return true;
    } catch (Exception e) {
      throw objectFailure("delete", path, e);
    }
  }

  @Override
  public boolean exists(ObjectPath path) {
    path = requirePath(path);
    try {
      return client.doesObjectExist(path.bucket(), path.key());
    } catch (Exception e) {
      throw objectFailure("check object existence", path, e);
    }
  }

  @Override
  public ListObjectsPage list(ListObjectsRequest request) {
    ListObjectsRequest validatedRequest = requireListRequest(request);
    try {
      com.obs.services.model.ListObjectsRequest obsRequest = new com.obs.services.model.ListObjectsRequest(
          validatedRequest.bucket());
      obsRequest.setPrefix(validatedRequest.prefix());
      obsRequest.setMaxKeys(validatedRequest.maxKeys());
      obsRequest.setMarker(validatedRequest.continuationToken());

      ObjectListing listing = client.listObjects(obsRequest);
      List<ObjectSummary> summaries = listing.getObjects().stream()
          .map(summary -> ObjectStorageValueFactory.objectSummary(
              validatedRequest.bucket(),
              summary.getObjectKey(),
              summary.getMetadata() == null ? 0 : summary.getMetadata().getContentLength(),
              null,
              null))
          .toList();

      return new ListObjectsPage(summaries, listing.getNextMarker(), listing.isTruncated());
    } catch (Exception e) {
      throw bucketFailure("list", validatedRequest.bucket(), e);
    }
  }

  @Override
  public ObjectWriteResult copy(ObjectPath source, ObjectPath target,
      ObjectMetadata metadataOverride) {
    source = requirePath(source);
    target = requirePath(target);
    try {
      CopyObjectRequest copyRequest = new CopyObjectRequest(source.bucket(), source.key(),
          target.bucket(), target.key());
      if (metadataOverride != null) {
        com.obs.services.model.ObjectMetadata meta = new com.obs.services.model.ObjectMetadata();
        ObjectMetadataMapper.apply(
            metadataOverride,
            meta::setContentType,
            ignored -> { },
            meta::setCacheControl,
            meta::setContentDisposition,
            userMetadata -> meta.setUserMetadata(new HashMap<>(userMetadata)));
        copyRequest.setNewObjectMetadata(meta);
      }
      CopyObjectResult result = client.copyObject(copyRequest);
      return new ObjectWriteResult(target, result.getEtag(), null);
    } catch (Exception e) {
      throw objectFailure("copy", source, e);
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
    return super.unwrap(type);
  }
}
