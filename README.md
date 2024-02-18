## 根据开源代码修改，开源Path如下
<https://github.com/mindis/prom2click>

## 下载代码

go编译在不同OS下会生成不同的执行文件，如果需要生成linux下可执行的文件，需要在linux上执行以下命令

```console
git clone http://git.vemic.com/fbi/fide/prom2click.git
```

## 项目编译

* glide是go的包管理工具；以下命令是初始化项目的glide.yml文件，类似于pom.xml；扫描项目dependencies，确定包版本，此过程需要几分钟。

```console
glide init
```

* 像java maven一样，下载go的各种包

```console
go mod tidy
go mod vendor
```

* 编译命令生成可执行文件

```console
go build -C ./src -o ../bin/prom2click
```
