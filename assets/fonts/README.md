在此目录放置可再分发的中文字库文件，可提升 Linux 或最小系统环境下的中文显示稳定性。

推荐文件名之一：

- `SourceHanSansSC-Regular.otf`
- `SourceHanSansCN-Regular.otf`
- `NotoSansCJKsc-Regular.otf`
- `NotoSansSC-Regular.otf`

日志和 JSON 编辑区会单独优先寻找等宽中文字库：

- `NotoSansMonoCJKsc-Regular.otf`
- `NotoSansMonoCJKSC-Regular.otf`
- `SarasaMonoSC-Regular.ttf`
- `SarasaMonoSC-Regular.otf`

运行时加载顺序：

1. 仓库或程序目录下 `assets/fonts`
2. 常见系统中文字体
3. 如果没有等宽 CJK 字体，则将比例中文字库作为 Monospace fallback，避免中文缺字
