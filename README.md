# mcap2dora

ROS 1/2 の MCAP rosbag を、dora-rs がメモリ共有(ゼロコピー)で扱える
**Apache Arrow IPC ファイル**(トピックごとに1ファイル)へ変換するツール。

dora-rs はノード間データを Arrow 配列として表現し、共有メモリ転送も Arrow
ベースで行うため、Arrow IPC ファイルは mmap するだけで dora ノード
(`dora-node-api` の `send_output`)にゼロコピーで載せられる。

## 変換モード

| モード | 内容 |
|---|---|
| `raw` | `log_time` / `publish_time` / `data`(CDRバイト列を LargeBinary のまま) |
| `decoded` | `log_time` / `publish_time` + 全フィールドを型付きArrow列に展開(ネストは Struct/List、`uint8[]` は LargeBinary) |

`decoded` は MCAP に埋め込まれた ros2msg / ros1msg スキーマを動的に解釈するので、
独自パッケージのカスタムメッセージ型もコード生成なしで展開できる。
スキーマが解釈できないトピックは自動的に raw にフォールバックする。

## ビルド(Docker)

```bash
git clone https://github.com/sige0002/mcap2dora.git && cd mcap2dora

# イメージをビルド(マルチステージ、実行イメージは bookworm-slim)
docker build -t mcap2dora .
```

開発時は rust イメージ + ボリュームマウントでもビルドできる:

```bash
docker run --rm -v $PWD:/app -v mcap2dora_cargo:/usr/local/cargo/registry \
  -w /app rust:1-bookworm cargo build --release
```

### プロキシ環境でのビルド

`docker build` は `http_proxy` / `https_proxy` / `no_proxy` を事前定義の
build-arg として認識する(値を省略するとホストの同名環境変数が渡る):

```bash
docker build -t mcap2dora \
  --build-arg http_proxy --build-arg https_proxy --build-arg no_proxy .
```

rust イメージで直接 `cargo build` する場合は `-e` で渡す:

```bash
docker run --rm -e http_proxy -e https_proxy -e no_proxy \
  -v $PWD:/app -v mcap2dora_cargo:/usr/local/cargo/registry \
  -w /app rust:1-bookworm cargo build --release
```

- ベースイメージの pull はビルドではなくデーモンが行うため、必要なら
  デーモン側のプロキシ設定(`/etc/systemd/system/docker.service.d/http-proxy.conf`
  など)を行う
- 変換の実行自体はネットワークアクセス不要なので、`docker run` にプロキシ設定は不要

## ライブラリとして使う(in-memory batch)

ファイルを書かずに、mcapをデコードした `RecordBatch` をメモリ上でそのまま
受け取れる。dora ノードに組み込む場合はこちらを使う(`send_output` に直接
渡せるので、ディスク往復が不要):

```toml
[dependencies]
mcap2dora = { git = "https://github.com/sige0002/mcap2dora" }
```

```rust
use mcap2dora::{map_file, McapArrowReader, Mode, ReaderOptions};

let mapped = map_file(std::path::Path::new("bag_0.mcap"))?;
let mut reader = McapArrowReader::new(&mapped, ReaderOptions {
    mode: Mode::Decoded,
    ..Default::default()
})?;
while let Some(tb) = reader.next_batch()? {
    // tb.topic / tb.msg_type / tb.batch (arrow::record_batch::RecordBatch)
    // → そのまま dora の send_output へ
}
println!("{:?}", reader.stats()); // messages / fallback / failed の内訳
```

- バッチ粒度は `max_batch_rows` / `max_batch_bytes` で調整(デフォルト 65536行 / 64MB)
- トピック内のバッチはメッセージ順、トピック間はフラッシュ順で混ざる
  (各行に `log_time` / `publish_time` 列があるので時刻順の再生はそれを使う)
- CLI の `convert` はこのAPIの出力をArrow IPCファイルに書くだけの薄いラッパ

## 任意のrosbagを変換する

mcapファイルを1つ指定すると、トピックごとの `.arrow` を出力ディレクトリに書く。
rosbagのあるディレクトリと出力先をマウントして実行するだけ:

```bash
docker run --rm \
  -v /path/to/your/rosbags:/data:ro \
  -v $PWD/output:/out \
  mcap2dora convert --mode decoded --out /out/mybag /data/mybag/mybag_0.mcap

# 検証(Arrowファイルを読み戻して行数を表示)
docker run --rm -v $PWD/output:/out mcap2dora verify /out/mybag
```

- ROS 2(cdr + ros2msg)/ ROS 1(ros1 + ros1msg)どちらのmcapにも対応
- カスタムメッセージ型も埋め込みスキーマから自動対応(コード生成・msgファイル不要)
- 出力サイズは入力mcapとほぼ同じ(非圧縮Arrow IPC)なので空き容量に注意

## ベンチマーク

`bench.sh` は `/rosbags` 以下の全mcapを両モードで変換し、1bagごとの計測値を
JSONで記録する(rosbagディレクトリを `/rosbags` にマウントして実行):

```bash
docker run --rm -v $PWD:/app -v /path/to/your/rosbags:/rosbags:ro \
  -v $PWD/bench_tmp:/bench_tmp -w /app rust:1-bookworm bash /app/bench.sh
```

- 結果は `results/bench_results.jsonl`(1行1JSON)
- 実行前に `cat` でページキャッシュを温めるので、計測値はディスクI/Oではなく
  変換処理(展開・デコード・書き出し)のスループット

### 計測結果(参考)

ROS 2 bag 33個・計29.1GB・221万メッセージを1台(20コア、NVMe)で変換した結果:

| モード | 出力先 | 合計時間 | スループット |
|---|---|---:|---:|
| raw | ファイル | 36.8s | 791 MB/s |
| decoded | ファイル | 37.1s | 785 MB/s |
| raw | **in-memory** | 14.3s | **2040 MB/s** |
| decoded | **in-memory** | 14.7s | **1984 MB/s** |

- **raw と decoded はほぼ同速**: データ量の大半を占める圧縮画像はどちらの
  モードでもバイト列コピーになるため、フィールド展開のコストは誤差範囲。
  型付き列が得られる decoded を推奨
- **in-memory(ライブラリ経路)はファイル出力の約2.5倍**。最大の6.7GB bagでも
  約3秒で全デコードできる。in-memory の計測は `drain` サブコマンドで再現可能:

```bash
docker run --rm -v /path/to/rosbags:/data:ro mcap2dora drain --mode decoded /data/xxx.mcap
```

## 出力フォーマット

各 `.arrow` は Arrow IPC File format。スキーマの custom metadata に
`mcap2dora:topic` / `mcap2dora:type` / `mcap2dora:mode` / `mcap2dora:message_encoding`
を持つ。dora の replay ノードからは mmap → `FileReader` で読み、行単位で
スライスして `send_output` すればよい。
