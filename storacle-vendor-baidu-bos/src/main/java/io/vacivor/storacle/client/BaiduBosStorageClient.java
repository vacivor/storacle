package io.vacivor.storacle.client;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.CopyObjectRequest;
import com.baidubce.services.bos.model.CopyObjectResponse;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.PutObjectRequest;
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
import java.util.List;

public final class BaiduBosStorageClient extends AbstractObjectStorageClient {

  private final BosClient client;

  public BaiduBosStorageClient(BosClient client) {
    this.client = java.util.Objects.requireNonNull(client, "client must not be null");
  }

  @Override
  public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
    path = requirePath(path);
    content = requireContent(content);
    ObjectMetadata safeMetadata = safeMetadata(metadata);

    try {
      com.baidubce.services.bos.model.ObjectMetadata bosMetadata = new com.baidubce.services.bos.model.ObjectMetadata();
      ObjectMetadataMapper.apply(
          safeMetadata,
          bosMetadata::setContentType,
          bosMetadata::setContentLength,
          bosMetadata::setCacheControl,
          bosMetadata::setContentDisposition,
          bosMetadata::setUserMetadata);

      PutObjectRequest request = new PutObjectRequest(path.bucket(), path.key(), content,
          bosMetadata);
      client.putObject(request);
      return new ObjectWriteResult(path, null, null);
    } catch (Exception e) {
      throw objectFailure("put", path, e);
    }
  }

  @Override
  public StorageObject get(ObjectPath path) {
    path = requirePath(path);
    try {
      BosObject object = client.getObject(path.bucket(), path.key());
      com.baidubce.services.bos.model.ObjectMetadata meta = object.getObjectMetadata();
      ObjectMetadata metadata = ObjectMetadataMapper.from(
          meta::getContentType,
          meta::getContentLength,
          meta::getCacheControl,
          meta::getContentDisposition,
          meta::getUserMetadata);
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
      com.baidubce.services.bos.model.ListObjectsRequest bosRequest = new com.baidubce.services.bos.model.ListObjectsRequest(
          validatedRequest.bucket())
          .withPrefix(validatedRequest.prefix())
          .withMaxKeys(validatedRequest.maxKeys())
          .withMarker(validatedRequest.continuationToken());
      ListObjectsResponse listObjectsResponse = client.listObjects(bosRequest);
      List<ObjectSummary> summaries = listObjectsResponse.getContents().stream()
          .map(summary -> ObjectStorageValueFactory.objectSummary(
              validatedRequest.bucket(),
              summary.getKey(),
              summary.getSize(),
              summary.getLastModified() == null ? null : summary.getLastModified().toInstant(),
              summary.getETag()))
          .toList();
      return new ListObjectsPage(summaries, listObjectsResponse.getNextMarker(),
          listObjectsResponse.isTruncated());
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
      CopyObjectRequest request = new CopyObjectRequest(source.bucket(), source.key(),
          target.bucket(), target.key());
      if (metadataOverride != null) {
        com.baidubce.services.bos.model.ObjectMetadata meta = new com.baidubce.services.bos.model.ObjectMetadata();
        ObjectMetadataMapper.apply(
            metadataOverride,
            meta::setContentType,
            ignored -> { },
            meta::setCacheControl,
            meta::setContentDisposition,
            meta::setUserMetadata);
        request.setNewObjectMetadata(meta);
      }
      CopyObjectResponse response = client.copyObject(request);
      return new ObjectWriteResult(target, response.getETag(), null);
    } catch (Exception e) {
      throw objectFailure("copy", source, e);
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
    return super.unwrap(type);
  }
}
