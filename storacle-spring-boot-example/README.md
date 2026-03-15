# Spring Boot Example

这个 example 展示了如何在 Spring Boot 应用里使用 `storacle-spring-boot-starter` 连接 MinIO，并暴露一组最小的对象存储接口。

## 运行方式

1. 启动一个本地 MinIO：

```bash
docker run --rm \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

2. 启动 example：

```bash
./gradlew :storacle-spring-boot-example:bootRun
```

如果你的 MinIO 地址或账号不同，可以覆盖：

```bash
STORACLE_MINIO_ENDPOINT=http://localhost:9000 \
STORACLE_MINIO_ACCESS_KEY=minioadmin \
STORACLE_MINIO_SECRET_KEY=minioadmin \
./gradlew :storacle-spring-boot-example:bootRun
```

这个 example 默认启用了：

- `storacle.content-type-detector=tika`
- `scene=avatar` 时只允许 `image/png`、`image/jpeg`，大小不超过 `2MB`
- `scene=document` 时只允许 `application/pdf`，大小不超过 `10MB`

检测器选择建议：

- `default`：依赖更轻，适合对类型识别精度要求不高、只想保守探测的场景
- `tika`：识别能力更强，适合需要更可靠内容识别和内容类型策略校验的场景

## 接口

### 上传文件

```bash
curl -X POST "http://localhost:8080/api/objects" \
  -F "bucket=test-bucket" \
  -F "prefix=demo/" \
  -F "file=@./README.md"
```

如果想在上传时一起计算校验值：

```bash
curl -X POST "http://localhost:8080/api/objects" \
  -F "bucket=test-bucket" \
  -F "scene=avatar" \
  -F "prefix=demo/" \
  -F "checksumAlgorithm=SHA-256" \
  -F "checksumAlgorithm=CRC32" \
  -F "file=@./avatar.png"
```

示例响应：

```json
{
  "bucket": "test-bucket",
  "key": "demo/README.md",
  "originalFilename": "README.md",
  "contentType": "text/markdown",
  "contentLength": 1234,
  "eTag": "9f620878e06a5508cc7f2a4091b8dfea",
  "versionId": null,
  "checksums": {
    "SHA-256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
    "CRC32": "3610a686"
  }
}
```

失败示例：`scene=avatar` 上传 PDF

```bash
curl -X POST "http://localhost:8080/api/objects" \
  -F "bucket=test-bucket" \
  -F "scene=avatar" \
  -F "file=@./contract.pdf"
```

示例错误响应：

```json
{
  "timestamp": "2026-03-15T12:00:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Content type not allowed: application/pdf",
  "path": "/api/objects"
}
```

失败示例：`scene=document` 上传超过 `10MB` 的文件

```bash
curl -X POST "http://localhost:8080/api/objects" \
  -F "bucket=test-bucket" \
  -F "scene=document" \
  -F "file=@./large-document.pdf"
```

示例错误响应：

```json
{
  "timestamp": "2026-03-15T12:00:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Content length exceeds limit: 12582912 > 10485760",
  "path": "/api/objects"
}
```

### 列出对象

```bash
curl "http://localhost:8080/api/objects?bucket=test-bucket&prefix=demo/"
```

### 检查对象是否存在

```bash
curl "http://localhost:8080/api/objects/exists?bucket=test-bucket&key=demo/your-file.txt"
```

### 下载对象

```bash
curl "http://localhost:8080/api/objects/content?bucket=test-bucket&key=demo/your-file.txt" -o downloaded-file.txt
```

### 查看对象 metadata

```bash
curl "http://localhost:8080/api/objects/metadata?bucket=test-bucket&key=demo/your-file.txt"
```

如果对象的 `userMetadata` 里包含约定格式的 checksum key，比如 `checksum-sha-256`、`checksum-crc32`，接口也会把它们解析到 `checksums` 字段里。

### 删除对象

```bash
curl -X DELETE "http://localhost:8080/api/objects?bucket=test-bucket&key=demo/your-file.txt"
```

## 说明

- 这个 example 使用 `ObjectStorageTemplate`，所以支持自动生成 key、文件名探测和 content-type 推断。
- 如果你的业务需要不同的 content-type 规则，可以通过 `scene` 配合 `ContentTypePolicyResolver` 在上传时动态路由策略。
- 如果你只是想按 `scene` 做简单路由，可以直接使用 `RoutingContentTypePolicyResolver`，按 `scene -> policy` 映射配置即可。
- 最常见的允许列表校验可以直接复用 `AllowlistContentTypePolicy`，比如头像只允许 `image/png` 和 `image/jpeg`。
- 如果一个 scene 需要叠加多种规则，可以用 `CompositeContentTypePolicy`，比如“类型允许列表 + 最大大小限制”。
- 最大内容长度限制可以直接复用 `MaxContentLengthPolicy`。
- starter 现在支持通过 `storacle.content-type-detector=default|tika` 切换检测器实现。
- `ObjectWriteResult` 现在支持 `checksum(...)` 和 `checksumOrNull(...)`，比直接操作 `checksums()` 更顺手。
- 上传时可以追加多个 `checksumAlgorithm` 参数，返回结果会按请求顺序输出。
- 如果你们要把 checksum 一起持久化到对象 metadata，可以使用 `ChecksumMetadata.userMetadataKey(...)` 和 `ChecksumMetadata.withChecksums(...)` 这套约定。
- 上传前请先在 MinIO 中创建目标 bucket，比如 `test-bucket`。
