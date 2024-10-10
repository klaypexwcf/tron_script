# Tron Script
本项目提供了用于测试波场p2p网络的脚本工具。功能包括波场节点发现协议消息收发和修改、节点连接、p2p网络消息监听和修改、p2p消息解析和打包等功能
## Module 1-MyConnection
该模块实现了波场p2p网络中各种消息类型的构造、打包、解析功能，p2p连接的握手、连接信道保活等机制。该模块支持自定义消息、参数修改、消息中继流程自定义、常量更改。

该模块可以用于测试波场全节点连接、探测波场节点信息、调试波场全节点连接等。
### 模块解释？//TODO//
### 编译
编译需要`git`和64位的`Oracle JDK 1.8`，其他版本的JDK尚不支持。`Windows`和`Linux`均可

clone仓库
```bash
$ git clone 仓库地址/Tron Script.git
$ cd ./文件夹/Myconnection
$ javac myChannelManager.java
```
在当前目录下创建一个名为`MANIFEST.MF`的文件

内容如下
### 运行
## Module 2-MyDiscover