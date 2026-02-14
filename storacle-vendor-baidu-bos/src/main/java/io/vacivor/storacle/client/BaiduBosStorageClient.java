package io.vacivor.storacle.client;

import java.util.Objects;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.CopyObjectRequest;
import com.baidubce.services.bos.model.CopyObjectResponse;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.PutObjectRequest;
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
import java.util.List;

public final class BaiduBosStorageClient implements ObjectStorageClient {

  private final BosClient client;

  public BaiduBosStorageClient(BosClient client) {
    this.client = Objects.requireNonNull(client, "client must not be null");
  }

  @Override
  public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
    Objects.requireNonNull(path, "path must not be null");
    Objects.requireNonNull(content, "content must not be null");
    ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;

    try {
      com.baidubce.services.bos.model.ObjectMetadata bosMetadata = new com.baidubce.services.bos.model.ObjectMetadata();
      if (safeMetadata.contentType() != null) {
        bosMetadata.setContentType(safeMetadata.contentType());
      }
      if (safeMetadata.contentLength() != null && safeMetadata.contentLength() >= 0) {
        bosMetadata.setContentLength(safeMetadata.contentLength());
      }
      if (safeMetadata.cacheControl() != null) {
        bosMetadata.setCacheControl(safeMetadata.cacheControl());
      }
      if (safeMetadata.contentDisposition() != null) {
        bosMetadata.setContentDisposition(safeMetadata.contentDisposition());
      }
      if (!safeMetadata.userMetadata().isEmpty()) {
        bosMetadata.setUserMetadata(safeMetadata.userMetadata());
      }

      PutObjectRequest request = new PutObjectRequest(path.bucket(), path.key(), content,
          bosMetadata);
      client.putObject(request);
      return new ObjectWriteResult(path, null, null);
    } catch (Exception e) {
      throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
    }
  }

  @Override
  public StorageObject get(ObjectPath path) {
    Objects.requireNonNull(path, "path must not be null");
    try {
      BosObject object = client.getObject(path.bucket(), path.key());
      com.baidubce.services.bos.model.ObjectMetadata meta = object.getObjectMetadata();
      ObjectMetadata metadata = ObjectMetadata.builder()
          .contentType(meta.getContentType())
          .contentLength(meta.getContentLength())
          .cacheControl(meta.getCacheControl())
          .contentDisposition(meta.getContentDisposition())
          .userMetadata(meta.getUserMetadata())
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
      com.baidubce.services.bos.model.ListObjectsRequest bosRequest = new com.baidubce.services.bos.model.ListObjectsRequest(
          request.bucket())
          .withPrefix(request.prefix())
          .withMaxKeys(request.maxKeys())
          .withMarker(request.continuationToken());
      ListObjectsResponse listObjectsResponse = client.listObjects(bosRequest);
      List<ObjectSummary> summaries = listObjectsResponse.getContents().stream()
          .map(summary -> new ObjectSummary(
              ObjectPath.of(request.bucket(), summary.getKey()),
              summary.getSize(),
              summary.getLastModified() == null ? null : summary.getLastModified().toInstant(),
              summary.getETag()))
          .toList();
      return new ListObjectsPage(summaries, listObjectsResponse.getNextMarker(),
          listObjectsResponse.isTruncated());
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
      CopyObjectRequest request = new CopyObjectRequest(source.bucket(), source.key(),
          target.bucket(), target.key());
      if (metadataOverride != null) {
        com.baidubce.services.bos.model.ObjectMetadata meta = new com.baidubce.services.bos.model.ObjectMetadata();
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
          meta.setUserMetadata(metadataOverride.userMetadata());
        }
        request.setNewObjectMetadata(meta);
      }
      CopyObjectResponse response = client.copyObject(request);
      return new ObjectWriteResult(target, response.getETag(), null);
    } catch (Exception e) {
      throw new StorageException("Failed to copy object: " + source.bucket() + "/" + source.key(),
          e);
    }
  }

  @Override
  public void close() {
    client.shutdown();
  }

  @Override
  public <T> T unwrap(Class<T> type) {
    if (type.isInstance(client)) {
      return type.cast(client);
    }
    return ObjectStorageClient.super.unwrap(type);
  }
}
