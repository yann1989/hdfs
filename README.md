# **hdfs-rest**

封装 HDFS 客户端, 使用 HDFS Restful 接口操作 HDFS.

## API Reference

### API Reference : Directory Operations

- **Mkdir**: 创建目录
- **MkdirAll**: 创建目录

### API Reference : File Operations

- **Create**: 按照默认参数创建文件
- **CreateFile**: 按照传入参数创建文件
- **Append**: 追加数据到指定文件
- **Read**: 读取整个文件
- **ReadFile**: 读取文件的某段数据

### API Reference : Status Operations

- **ListStatus**: 当前路径下的所有文件(所有类型的文件)
- **GetFileStatus**: 获取指定文件状态
- **Walk**: 遍历指定路径下的所有文件(所有类型的文件)
- **FileStatus.IsDir** 判断是否为目录


