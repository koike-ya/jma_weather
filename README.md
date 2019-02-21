手順書とデータ構造について記載する

ソースコードをクローンする
```
git clone https://github.com/Tomoya-K-0504/jma_weather.git
cd jma_weather
```

pythonの仮想環境を作成する。anacondaを使う場合を記載する。
```
conda -V # condaが使えることを確認
conda create -n weather python=3.6
source activate weather
```

必要なライブラリをインストールする
```
pip install -r requirements.txt
```

ここで、下記に記載の フォルダ構成 のように、郵便番号が書かれたファイルをweather直下に配置する

実行
```
python zip2weather.py zip20181105.xlsx # 引数には郵便番号を読み込ませるエクセルファイル名を指定 
```

## フォルダ構造
```
weather
├── JmaScraper.py # 気象庁のページからとってきたHTMLを整形・抽出するクラス
├── KEN_ALL.CSV # 県名、都市名と郵便番号が書かれた、郵便局のデータ
├── README.md # リポジトリの説明 
├── const.py # 定数やJMAのHTMLをDataFrameにしたときのカラム名を保存
├── logger.py # ログの設定ファイル
├── requirements.txt # 必要なライブラリをまとめたファイル
├── data # 取得した天気データを格納するフォルダ
│   └── 1_000000 # ID毎にフォルダが作られる
│       ├── daily.csv # 一日の天気のCSV
│       ├── end.csv # 計測の終了時の前後と当日24hの記録
│       └── start.csv　# 計測の開始時の前後と当日24hの記録
├── zip20180000.xlsx # 天気の取得に使用する郵便番号が書かれたエクセルファイル
└── zip2weather.py # 実行ファイル
``` 