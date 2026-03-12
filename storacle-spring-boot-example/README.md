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

## 接口

### 上传文件

```bash
curl -X POST "http://localhost:8080/api/objects" \
  -F "bucket=test-bucket" \
  -F "prefix=demo/" \
  -F "file=@./README.md"
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

### 删除对象

```bash
curl -X DELETE "http://localhost:8080/api/objects?bucket=test-bucket&key=demo/your-file.txt"
```

## 说明

- 这个 example 使用 `ObjectStorageTemplate`，所以支持自动生成 key、文件名探测和 content-type 推断。
- 上传前请先在 MinIO 中创建目标 bucket，比如 `test-bucket`。
